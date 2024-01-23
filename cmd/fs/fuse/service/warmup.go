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

	progress, bar := utils.NewDynProgressBar("warming up paths: ", false, int64(len(paths)))
	for _, path := range paths {
		path_ := path
		_ = pool.Submit(func() {
			if strings.HasSuffix(path_, "/") {
				if recursive {
					concurrentRecursiveWalk(path_)
				} else {
					warmupDir(path_)
				}
			} else {
				if warmType == "meta" {
					warmupMeta(path_)
				} else if warmType == "data" {
					warmupData(path_)
				} else {
					log.Fatal("type of warmup must meta or data")
				}
			}
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

// preheatRecursive 用于递归预热目录
//func preheatRecursive(dirPath string) error {
//	fmt.Println("Recursively preheating directory:", dirPath)
//	// 递归遍历目录
//	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
//		if err != nil {
//			fmt.Println("Error:", err)
//			return err
//		}
//		fmt.Println(path)
//		return nil
//	})
//
//	if err != nil {
//		fmt.Println("Error during WalkDir:", err)
//	}
//	return err
//}

func concurrentRecursiveWalk(root string) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	// 启动一个 goroutine 处理错误
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// 启动多个 goroutine 并发递归遍历和处理任务
	err := walkAndProcess(root, &wg, errCh)
	if err != nil {
		return err
	}

	// 等待所有 goroutine 完成
	wg.Wait()

	// 检查并发过程中是否出现错误
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func walkAndProcess(path string, wg *sync.WaitGroup, errCh chan error) error {
	wg.Add(1)
	_ = pool.Submit(func() {
		defer wg.Done()
		dirEntries, err := os.ReadDir(path)
		if err != nil {
			fmt.Printf("warmup path[%s] fail with err %v \n", path, err)
			return
		}
		fmt.Printf("warmup path[%v] success \n", path)
		for _, entry := range dirEntries {
			fullPath := filepath.Join(path, entry.Name())
			entry_ := entry
			_ = pool.Submit(func() {
				if entry_.IsDir() {
					// 递归调用
					walkAndProcess(fullPath, wg, errCh)
				}
			})
		}
	})
	return nil
}
