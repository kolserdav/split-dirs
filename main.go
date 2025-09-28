package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
)

const (
	DELETE_MIN_SIZE = 189 * 1024
	DIR_LENGHT_MAX  = 500
)

type VideoHash struct {
	FilePath string
	Stream   string
	Errors   int
}

type DelFiles struct {
	Orig   string
	Copies []string
}

func findTheSameFiles(file VideoHash, data []VideoHash) (VideoHash, []VideoHash) {
	res := make([]VideoHash, 0)

	cur := file
	for _, f := range data {
		if cur.FilePath == f.FilePath {
			continue
		}
		if file.Stream == f.Stream {
			if file.Errors > f.Errors {
				res = append(res, cur)
				cur = f
			}
			res = append(res, f)
		}
	}
	return cur, res
}

func deduplicateVideos(ch chan VideoHash, wg *sync.WaitGroup, dirPath string) {
	files := make([]VideoHash, 0)
	for msg := range ch {
		files = append(files, msg)
	}

	delFiles := make([]DelFiles, 0)
	for _, f := range files {
		cur, same := findTheSameFiles(f, files)
		if len(same) > 0 {
			copies := make([]string, 0)
			for _, s := range same {
				copies = append(copies, s.FilePath)
			}
			delFiles = append(delFiles, DelFiles{
				Orig:   cur.FilePath,
				Copies: copies,
			})
		}
	}

	for _, f := range delFiles {
		if f.Orig != "/mnt/deb/kol/recovery/VIDEO/MPEG/01142222.mts" {
			continue
		}
		fmt.Println("del", f.Orig, f.Copies)
	}
}

func getDuplicateVideos(ch chan VideoHash, wg *sync.WaitGroup, dirPath string) {
	defer wg.Done()
	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	streamReg, err := regexp.Compile(`(?s)\[STREAM\]\s*.*`)
	if err != nil {
		panic(err)
	}

	errorsReg, err := regexp.Compile(`\[[a-zA-Z0-9]+ @ 0x[a-fA-F0-9]+\] .+`)
	if err != nil {
		panic(err)
	}

	for _, file := range files {

		fileName := file.Name()
		filePath := path.Join(dirPath, fileName)

		info, err := os.Stat(filePath)
		if err != nil {
			panic(err)
		}
		if info.IsDir() {
			//wg.Add(1)
			//go deleteDuplicateVideos(wg, path.Join(dirPath, fileName))
			continue
		}

		cmd := exec.Command("ffprobe", "-v", "error", "-show_format", "-show_streams", filePath)

		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println(1, string(output))
			panic(err)
		}

		filenameReg, err := regexp.Compile(`filename=.+\n`)
		if err != nil {
			panic(err)
		}
		data := string(output)
		out := filenameReg.FindString(data)
		if out == "" {
			fmt.Println("can not get filename from video metadata", filePath, data)
			continue
		}

		stream := streamReg.FindString(data)
		if stream == "" {
			fmt.Println("can not get stream for file:", filePath, data)
			continue
		}

		errors := errorsReg.FindAllString(data, -1)

		ch <- VideoHash{
			FilePath: filePath,
			Stream:   strings.Replace(data, stream, "", 1),
			Errors:   len(errors),
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

	command := args[1]
	dirPath := args[2]
	switch command {
	case "name":
		wg.Add(1)
		go splitByName(&wg, dirPath)
		wg.Wait()
	case "lenght":
		wg.Add(1)
		go splitByLenght(&wg, dirPath)
		wg.Wait()
	case "video":
		ch := make(chan VideoHash)
		go func() {
			wg.Wait()
			close(ch)
		}()
		wg.Add(1)
		go getDuplicateVideos(ch, &wg, dirPath)
		deduplicateVideos(ch, &wg, dirPath)

	default:
		fmt.Printf("command '%s' is not allowed; allowed commands (name|lenght|video)", command)
		wg.Done()
	}

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
