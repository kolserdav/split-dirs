package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	DELETE_MIN_SIZE = 189 * 1024
	DIR_LENGHT_MAX  = 500
)

type VideoFile struct {
	FilePath  string
	TimeStart int64
	TimeEnd   int64
	Duration  int64
}

func isSame(cur VideoFile, new VideoFile) bool {
	curMin := cur.TimeStart
	curMax := cur.TimeEnd
	return curMax >= new.TimeEnd && curMin <= new.TimeStart
}

func findTheSameFiles(file VideoFile, data []VideoFile) (VideoFile, []VideoFile) {
	res := make([]VideoFile, 0)

	cur := file
	for _, f := range data {
		if cur.FilePath == f.FilePath {
			continue
		}
		isLarge := isSame(f, cur)
		if isSame(cur, f) || isLarge {
			if isLarge {
				res = append(res, cur)
				cur = f
			} else {
				res = append(res, f)
			}

		}
	}

	return cur, res
}

func deduplicateVideos(ch chan VideoFile) {
	files := make([]VideoFile, 0)
	for msg := range ch {
		files = append(files, msg)
	}

	delFiles := make(map[string][]VideoFile, 0)
	for _, f := range files {
		if _, ok := delFiles[f.FilePath]; !ok {
			delFiles[f.FilePath] = make([]VideoFile, 0)
		} else {
			continue
		}
		cur, same := findTheSameFiles(f, files)
		if len(same) > 0 {
			copies := make([]VideoFile, 0)
			for _, s := range same {
				copies = append(copies, s)
			}
			if cur.FilePath != f.FilePath {
				delFiles[cur.FilePath] = copies
			} else {
				delFiles[f.FilePath] = copies
			}

		}
	}
	for _, f := range delFiles {
		for _, delF := range f {
			fmt.Println("remove file with no times", delF.FilePath)
			err := os.Remove(delF.FilePath)
			if err != nil {
				fmt.Println("failed to remove:", err)
			}
		}
	}
}

func getDuplicateVideos(ch chan VideoFile, wg *sync.WaitGroup, dirPath string) {
	defer wg.Done()

	layout := "2006-01-02 15:04:05"

	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	dateReg, err := regexp.Compile(`(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})`)
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
			wg.Add(1)
			go getDuplicateVideos(ch, wg, path.Join(dirPath, fileName))
			continue
		}

		cmd := exec.Command("exiftool", "-d", "'%Y-%m-%d %H:%M:%S'", "-DateTimeOriginal ", "-ExtractEmbedded", filePath)

		output, err := cmd.Output()
		if err != nil {
			fmt.Println(1, string(output))
			panic(err)
		}

		data := string(output)
		dates := strings.Split(data, "\n")
		times := make([]time.Time, 0)
	inner:
		for _, date := range dates {
			if date == "" {
				continue inner
			}
			d := dateReg.FindString(data)
			if d == "" {
				fmt.Println("failed to get date for file:", filePath, date)
				continue inner
			}
			time, err := time.Parse(layout, d)
			if err != nil {
				fmt.Println("failed to parse time for file:", filePath, err)
				continue inner
			}

			times = append(times, time)
		}

		if len(times) == 0 {
			err := os.Remove(filePath)
			if err != nil {
				fmt.Println("failed to remove file without times", err)
			}
			continue
		}
		var timeL int64 = int64(len(times))
		ch <- VideoFile{
			FilePath:  filePath,
			TimeStart: times[0].Unix(),
			TimeEnd:   times[timeL-1].Unix(),
			Duration:  timeL,
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
	case "mpeg":
		ch := make(chan VideoFile)
		go func() {
			wg.Wait()
			close(ch)
		}()
		wg.Add(1)
		go getDuplicateVideos(ch, &wg, dirPath)
		deduplicateVideos(ch)

	default:
		fmt.Printf("command '%s' is not allowed; allowed commands (name|lenght|mpeg)", command)
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
			err := os.Remove(filePath)
			if err != nil {
				fmt.Println("failed to remove:", err)
			}
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
