package service

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
)

func warmup(ctx *cli.Context) error {
	fname := ctx.String("file")
	paths := ctx.Args().Slice()
	threads := int(ctx.Uint("threads"))
	warmType := ctx.String("type")
	return warmup_(fname, paths, threads, warmType)
}

func warmup_(fname string, paths []string, threads int, warmType string) error {
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

	pool, _ := ants.NewPool(threads)
	progress, bar := utils.NewDynProgressBar("warming up paths: ", false)
	bar.SetTotal(int64(len(paths)), false)
	for _, path := range paths {
		path_ := path
		_ = pool.Submit(func() {
			if warmType == "meta" {
				warmupMeta(path_)
			} else if warmType == "data" {
				warmupData(path_)
			} else {
				log.Fatal("type of warmup must meta or data")
			}
			bar.IncrBy(1)
		})
	}
	bar.SetTotal(0, true)
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
				Value:   50,
				Usage:   "number of concurrent workers",
			},
			&cli.StringFlag{
				Name:    "type",
				Aliases: []string{"t"},
				Value:   "meta",
				Usage:   "type of warmup, e.g. meta, data",
			},
		},
	}
}
