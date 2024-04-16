package service

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
)

var pool *ants.Pool

const batchSize = 100000
const poolSize = 100
const minxFileCount = 10

var processNum int
var processNumLock sync.RWMutex

// findUniqueParentDirs 从 paths 中找出所有的父目录，如果父目录下的文件数量小于 minxFileCount，则将其下的文件都加入到结果中
func findUniqueParentDirs(paths []string) []string {
	var wg sync.WaitGroup
	var rwmu sync.RWMutex
	parentDirMap := make(map[string]map[string]struct{})

	// 协程任务
	processBatch := func(pathBatch []string) {
		for _, p := range pathBatch {
			// 忽略目录，只处理文件
			if p[len(p)-1] == '/' {
				continue
			}
			// 获取文件的父目录
			dir := filepath.Dir(p)
			if dir[len(dir)-1] != '/' {
				dir += "/"
			}

			rwmu.RLock()
			dirMap, exists := parentDirMap[dir]
			rwmu.RUnlock()

			if !exists {
				rwmu.Lock()
				parentDirMap[dir] = make(map[string]struct{})
				parentDirMap[dir][p] = struct{}{}
				rwmu.Unlock()
			} else if exists && len(dirMap) < minxFileCount {
				rwmu.Lock()
				parentDirMap[dir][p] = struct{}{}
				rwmu.Unlock()
			}
		}
		wg.Done()
	}

	log.Infof("Start to find unique parent dirs")

	// 分批提交协程池处理
	if len(paths) <= batchSize*poolSize {
		// 总数据量低于预设
		batchCount := (len(paths) + batchSize - 1) / batchSize
		for i := 0; i < batchCount; i++ {
			start := i * batchSize
			end := (i + 1) * batchSize
			if end > len(paths) {
				end = len(paths)
			}
			wg.Add(1)
			_ = pool.Submit(func() {
				processBatch(paths[start:end])
			})
		}
	} else {
		newBatchSize := (len(paths) + poolSize - 1) / poolSize
		for i := 0; i < poolSize; i++ {
			start := i * newBatchSize
			end := (i + 1) * newBatchSize
			if end > len(paths) {
				end = len(paths)
			}
			wg.Add(1)
			_ = pool.Submit(func() {
				processBatch(paths[start:end])
			})
		}
	}

	wg.Wait()

	// 从 parentDirMap 中提取结果
	uniqueParentDirs := make([]string, 0)
	for parentPath, dirMap := range parentDirMap {
		if len(dirMap) >= minxFileCount {
			uniqueParentDirs = append(uniqueParentDirs, parentPath)
		} else {
			for filePath, _ := range dirMap {
				uniqueParentDirs = append(uniqueParentDirs, filePath)
			}
		}
	}
	log.Infof("Found %d unique parent dirs", len(uniqueParentDirs))
	return uniqueParentDirs
}

func warmup(ctx *cli.Context) error {
	fname := ctx.String("file")
	paths := ctx.Args().Slice()
	threads := int(ctx.Uint("threads"))
	warmType := ctx.String("type")
	recursive := ctx.Bool("recursive")
	pool, _ = ants.NewPool(threads)
	return warmup_(fname, paths, threads, warmType, recursive)
}

func warmup_(fname string, paths []string, threads int, warmType string, recursive bool) error {
	now := time.Now()
	fd, err := os.Open(fname)
	if err != nil {
		log.Errorf("Failed to open file %s: %s", fname, err)
		return err
	}
	defer func() {
		_ = fd.Close()
	}()
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		if p := strings.TrimSpace(scanner.Text()); p != "" {
			paths = append(paths, p)
		}
	}
	if err = scanner.Err(); err != nil {
		log.Errorf("Reading file %s failed with error: %s", fname, err)
		return err
	}
	if len(paths) == 0 {
		log.Infof("Nothing to warm up")
		return nil
	}

	if warmType != "data" {
		paths = findUniqueParentDirs(paths)
	}
	log.Infof("warmup paths num: %v threads: %v warmupType: %v", len(paths), threads, warmType)

	progress, bar := utils.NewDynProgressBar("warming up paths: ", false, int64(len(paths)))
	for _, path := range paths {
		path_ := path
		if recursive && strings.HasSuffix(path_, "/") {
			concurrentRecursiveWalk(path_)
		}
		_ = pool.Submit(func() {
			if strings.HasSuffix(path_, "/") {
				warmupDir(path_)
			} else {
				if warmType == "meta" {
					warmupMeta(path_)
				} else if warmType == "data" {
					warmupData(path_)
				} else {
					log.Fatal("type of warmup must meta or data")
				}
			}
			processNumLock.Lock()
			processNum += 1
			if processNum%100 == 0 {
				log.Infof("Processing %v/%v", processNum, len(paths))
			}
			processNumLock.Unlock()

			bar.IncrBy(1)
		})
	}
	progress.Wait()
	fmt.Printf("spend time %v \n", time.Since(now))
	return nil
}

func warmupMeta(fileName string) {
	_, err := os.Stat(fileName)
	if err != nil {
		log.Errorf("Stat file %s with error: %v", fileName, err)
	}
}

func warmupDir(dirName string) {
	_, err := os.ReadDir(dirName)
	if err != nil {
		log.Errorf("ReadDir %s with error: %v", dirName, err)
	}
}

func warmupData(fileName string) {
	fh, err := os.Open(fileName)
	if err != nil {
		log.Errorf("Open file %s with error: %v", fileName, err)
		return
	}
	_, err = ioutil.ReadAll(fh)
	if err != nil {
		log.Errorf("ReadAll file %s with error: %v", fileName, err)
	}
}

func CmdWarmup() *cli.Command {
	return &cli.Command{
		Name:      "warmup",
		Category:  "TOOL",
		Usage:     "Build cache for target directories/files",
		ArgsUsage: "[PATH ...]",
		Action:    warmup,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "file",
				Required: true,
				Aliases:  []string{"f"},
				Usage:    "file containing a list of paths",
			},
			&cli.UintFlag{
				Name:    "threads",
				Aliases: []string{"p"},
				Value:   100,
				Usage:   "number of concurrent workers",
			},
			&cli.StringFlag{
				Name:    "type",
				Aliases: []string{"t"},
				Value:   "meta",
				Usage:   "type of warmup, e.g. meta, data",
			},
			&cli.BoolFlag{
				Name:    "recursive",
				Aliases: []string{"r"},
				Value:   false,
				Usage:   "enable recursive preheating",
			},
		},
	}
}

func concurrentRecursiveWalk(root string) error {
	// 启动多个 goroutine 并发递归遍历和处理任务
	err := walkAndProcess(root)
	if err != nil {
		return err
	}
	return nil

}

func walkAndProcess(path string) error {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		fmt.Printf("warmup path[%s] fail with err %v \n", path, err)
		return err
	}
	fmt.Printf("warmup path[%v] success \n", path)
	var wg sync.WaitGroup
	poolDir, _ := ants.NewPool(10)
	for _, entry := range dirEntries {
		fullPath := filepath.Join(path, entry.Name())
		entry_ := entry
		if entry_.IsDir() {
			wg.Add(1)
			// 递归调用
			_ = poolDir.Submit(func() {
				defer wg.Done()
				walkAndProcess(fullPath)
			})
		}
	}
	wg.Wait()
	return nil
}
