package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
)

const (
	DELETE_MIN_SIZE = 189 * 1024
	DIR_LENGHT_MAX  = 500
)

func splitDir(dirPathBase string, dirPath string, files []string) {
	_, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(dirPath, os.ModePerm)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	for _, file := range files {
		filePath := path.Join(dirPathBase, file)
		filePathNew := path.Join(dirPath, file)
		err = os.Rename(filePath, filePathNew)
		if err != nil {
			panic(err)
		}
	}
}

func splitByLenght(wg *sync.WaitGroup, dirPath string) {
	defer wg.Done()
	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		fileName := file.Name()
		filePath := path.Join(dirPath, fileName)

		hideReg, err := regexp.Compile(`^\.`)
		if err != nil {
			panic(err)
		}

		if hideReg.MatchString(fileName) {
			fmt.Println("skipping hidded", filePath)
			continue
		}

		info, err := os.Stat(filePath)
		if err != nil {
			panic(err)
		}
		if info.IsDir() {
			wg.Add(1)
			go splitByLenght(wg, path.Join(dirPath, fileName))
			continue
		}

	}
	lenght := len(files)

	if lenght > DIR_LENGHT_MAX {
		index := 0
		curFile := 0
		fileNames := make([]string, 0)
		for _, file := range files {
			curFile++
			index++
			fileNames = append(fileNames, file.Name())
			if curFile == DIR_LENGHT_MAX {
				splitDir(dirPath, path.Join(dirPath, fmt.Sprintf("i-%d-%d", index-DIR_LENGHT_MAX, index)), fileNames)
				fileNames = make([]string, 0)
				curFile = 0
			}
		}

	}
}

func main() {
	args := os.Args
	if len(args) != 3 {
		panic("wrong number of arguments: usage 'cli command /path'")
	}

	var wg sync.WaitGroup

	wg.Add(1)

	command := args[1]
	dirPath := args[2]
	switch command {
	case "name":
		go splitByName(&wg, dirPath)
	case "lenght":
		go splitByLenght(&wg, dirPath)
	default:
		fmt.Printf("command '%s' is not allowed; allowed commands (name|lenght)", command)
		wg.Done()
	}

	wg.Wait()
}

func splitByName(wg *sync.WaitGroup, dirPath string) {
	defer wg.Done()

	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	newDirs := make([]string, 0)
	for _, file := range files {
		fileName := file.Name()
		filePath := path.Join(dirPath, fileName)

		hideReg, err := regexp.Compile(`^\.`)
		if err != nil {
			panic(err)
		}

		if hideReg.MatchString(fileName) {
			fmt.Println("skipping hidded", filePath)
			continue
		}

		info, err := os.Stat(filePath)
		if err != nil {
			panic(err)
		}
		if info.IsDir() {
			wg.Add(1)
			go splitByName(wg, path.Join(dirPath, fileName))
			continue
		}

		if info.Size() < DELETE_MIN_SIZE {
			fmt.Println("try to delete", filePath)
			os.Remove(filePath)
		}

		prefixReg, err := regexp.Compile(`^[a-zA-Z0-9 ]+_`)
		if err != nil {
			panic(err)
		}
		prefix := prefixReg.FindString(fileName)

		if prefix == "" {
			continue
		}

		dirnameRaw := path.Join(dirPath, prefix)
		dirname := strings.Replace(dirnameRaw, "_", "", 1)
		_, err = os.Stat(dirname)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Println("try to create dir", dirname)
				err = os.Mkdir(dirname, os.ModePerm)
				if err != nil {
					panic(err)
				}
				newDirs = append(newDirs, dirname)

			} else {
				panic(err)
			}
		}

		filenameNew := strings.Replace(fileName, prefix, "", 1)
		err = os.Rename(filePath, path.Join(dirname, filenameNew))
		if err != nil {
			panic(err)
		}
	}

	for _, dir := range newDirs {
		wg.Add(1)
		go splitByName(wg, dir)
	}
}
