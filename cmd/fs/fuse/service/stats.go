/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"paddleflow/cmd/fs/fuse/flag"
	"paddleflow/pkg/common/config"
	mountutil "paddleflow/pkg/fs/utils/mount"
)

func CmdStats() *cli.Command {
	compoundFlags := [][]cli.Flag{
		flag.LogFlags(&config.FuseConf.Log),
		flag.MetricsFlags(&config.FuseConf.Fuse),
	}
	return &cli.Command{
		Name:      "stats",
		Action:    stats,
		Category:  "INSPECTOR",
		Usage:     "Show real time performance statistics of pfs-fuse on a mount point",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This is a tool that reads Prometheus metrics and shows real time statistics of the target mount point.

Examples:
$ pfs-fuse stats /mnt/mount_point

# More metrics
$ pfs-fuse stats /mnt/mount_point -l 1`,
		Flags: flag.ExpandFlags(compoundFlags),
	}
}

const (
	BLACK = 30 + iota
	RED
	GREEN
	YELLOW
	BLUE
	MAGENTA
	CYAN
	WHITE
)

const (
	RESET_SEQ      = "\033[0m"
	COLOR_SEQ      = "\033[1;" // %dm
	COLOR_DARK_SEQ = "\033[0;" // %dm
	UNDERLINE_SEQ  = "\033[4m"
	// BOLD_SEQ       = "\033[1m"
)

type statsWatcher struct {
	tty      bool
	interval uint
	path     string
	header   string
	sections []*section
}

func (w *statsWatcher) colorize(msg string, color int, dark bool, underline bool) string {
	if !w.tty || msg == "" || msg == " " {
		return msg
	}
	var cseq, useq string
	if dark {
		cseq = COLOR_DARK_SEQ
	} else {
		cseq = COLOR_SEQ
	}
	if underline {
		useq = UNDERLINE_SEQ
	}
	return fmt.Sprintf("%s%s%dm%s%s", useq, cseq, color, msg, RESET_SEQ)
}

const (
	metricByte = 1 << iota
	metricCount
	metricTime
	metricCPU
	metricGauge
	metricCounter
	metricHist
)

type item struct {
	nick string // must be size <= 5
	name string
	typ  uint8
}

type section struct {
	name  string
	items []*item
}

func (w *statsWatcher) buildSchema(schema string, verbosity uint) {
	for _, r := range schema {
		var s section
		switch r {
		case 'u':
			s.name = "usage"
			s.items = append(s.items, &item{"cpu", "pfs_cpu_usage", metricCPU | metricCounter})
			s.items = append(s.items, &item{"mem", "pfs_memory", metricGauge})
			s.items = append(s.items, &item{"buf", "pfs_used_buffer_size_bytes", metricGauge})
			if verbosity > 0 {
				s.items = append(s.items, &item{"cache", "pfs_store_cache_size_bytes", metricGauge})
			}
		case 'f':
			s.name = "fuse"
			s.items = append(s.items, &item{"ops", "pfs_fuse_ops_durations_histogram_seconds", metricTime | metricHist})
			s.items = append(s.items, &item{"read", "pfs_fuse_read_size_bytes_sum", metricByte | metricCounter})
			s.items = append(s.items, &item{"write", "pfs_fuse_written_size_bytes_sum", metricByte | metricCounter})
		case 'm':
			s.name = "meta"
			s.items = append(s.items, &item{"ops", "pfs_meta_ops_durations_histogram_seconds", metricTime | metricHist})
			if verbosity > 0 {
				s.items = append(s.items, &item{"txn", "pfs_transaction_durations_histogram_seconds", metricTime | metricHist})
				s.items = append(s.items, &item{"retry", "pfs_transaction_restart", metricCount | metricCounter})
			}
		case 'c':
			s.name = "blockcache"
			s.items = append(s.items, &item{"read", "pfs_blockcache_hit_bytes", metricByte | metricCounter})
			s.items = append(s.items, &item{"write", "pfs_blockcache_write_bytes", metricByte | metricCounter})
		case 'o':
			s.name = "object"
			s.items = append(s.items, &item{"get", "pfs_object_request_data_bytes_GET", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"get_c", "pfs_object_request_durations_histogram_seconds_GET", metricTime | metricHist})
			}
			s.items = append(s.items, &item{"put", "pfs_object_request_data_bytes_PUT", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"put_c", "pfs_object_request_durations_histogram_seconds_PUT", metricTime | metricHist})
				s.items = append(s.items, &item{"del_c", "pfs_object_request_durations_histogram_seconds_DELETE", metricTime | metricHist})
			}
		case 'g':
			s.name = "go"
			s.items = append(s.items, &item{"alloc", "pfs_go_memstats_alloc_bytes", metricGauge})
			s.items = append(s.items, &item{"sys", "pfs_go_memstats_sys_bytes", metricGauge})
		default:
			fmt.Printf("Warning: no item defined for %c\n", r)
			continue
		}
		w.sections = append(w.sections, &s)
	}
	if len(w.sections) == 0 {
		log.Fatalln("no section to watch, please check the schema string")
	}
}

func padding(name string, width int, char byte) string {
	pad := width - len(name)
	if pad < 0 {
		pad = 0
		name = name[0:width]
	}
	prefix := (pad + 1) / 2
	buf := make([]byte, width)
	for i := 0; i < prefix; i++ {
		buf[i] = char
	}
	copy(buf[prefix:], name)
	for i := prefix + len(name); i < width; i++ {
		buf[i] = char
	}
	return string(buf)
}

func (w *statsWatcher) formatHeader() {
	headers := make([]string, len(w.sections))
	subHeaders := make([]string, len(w.sections))
	for i, s := range w.sections {
		subs := make([]string, 0, len(s.items))
		for _, it := range s.items {
			subs = append(subs, w.colorize(padding(it.nick, 5, ' '), BLUE, false, true))
			if it.typ&metricHist != 0 {
				if it.typ&metricTime != 0 {
					subs = append(subs, w.colorize(" lat ", BLUE, false, true))
				} else {
					subs = append(subs, w.colorize(" avg ", BLUE, false, true))
				}
			}
		}
		width := 6*len(subs) - 1 // nick(5) + space(1)
		subHeaders[i] = strings.Join(subs, " ")
		headers[i] = w.colorize(padding(s.name, width, '-'), BLUE, true, false)
	}
	w.header = fmt.Sprintf("%s\n%s", strings.Join(headers, " "),
		strings.Join(subHeaders, w.colorize("|", BLUE, true, false)))
}

func (w *statsWatcher) formatU64(v float64, dark, isByte bool) string {
	if v <= 0.0 {
		return w.colorize("   0 ", BLACK, false, false)
	}
	var vi uint64
	var unit string
	var color int
	switch vi = uint64(v); {
	case vi < 10000:
		if isByte {
			unit = "B"
		} else {
			unit = " "
		}
		color = RED
	case vi>>10 < 10000:
		vi, unit, color = vi>>10, "K", YELLOW
	case vi>>20 < 10000:
		vi, unit, color = vi>>20, "M", GREEN
	case vi>>30 < 10000:
		vi, unit, color = vi>>30, "G", BLUE
	case vi>>40 < 10000:
		vi, unit, color = vi>>40, "T", MAGENTA
	default:
		vi, unit, color = vi>>50, "P", CYAN
	}
	return w.colorize(fmt.Sprintf("%4d", vi), color, dark, false) +
		w.colorize(unit, BLACK, false, false)
}

func (w *statsWatcher) formatTime(v float64, dark bool) string {
	var ret string
	var color int
	switch {
	case v <= 0.0:
		ret, color, dark = "   0 ", BLACK, false
	case v < 10.0:
		ret, color = fmt.Sprintf("%4.2f ", v), GREEN
	case v < 100.0:
		ret, color = fmt.Sprintf("%4.1f ", v), YELLOW
	case v < 10000.0:
		ret, color = fmt.Sprintf("%4.f ", v), RED
	default:
		ret, color = fmt.Sprintf("%1.e", v), MAGENTA
	}
	return w.colorize(ret, color, dark, false)
}

func (w *statsWatcher) formatCPU(v float64, dark bool) string {
	var ret string
	var color int
	switch v = v * 100.0; {
	case v <= 0.0:
		ret, color = " 0.0", WHITE
	case v < 30.0:
		ret, color = fmt.Sprintf("%4.1f", v), GREEN
	case v < 100.0:
		ret, color = fmt.Sprintf("%4.1f", v), YELLOW
	default:
		ret, color = fmt.Sprintf("%4.f", v), RED
	}
	return w.colorize(ret, color, dark, false) +
		w.colorize("%", BLACK, false, false)
}

func (w *statsWatcher) printDiff(left, right map[string]float64, dark bool) {
	if !w.tty && dark {
		return
	}
	fmt.Printf("cmd stats printDiff")
	values := make([]string, len(w.sections))
	for i, s := range w.sections {
		vals := make([]string, 0, len(s.items))
		for _, it := range s.items {
			switch it.typ & 0xF0 {
			case metricGauge: // currently must be metricByte
				vals = append(vals, w.formatU64(right[it.name], dark, true))
			case metricCounter:
				v := (right[it.name] - left[it.name])
				if !dark {
					v /= float64(w.interval)
				}
				if it.typ&metricByte != 0 {
					vals = append(vals, w.formatU64(v, dark, true))
				} else if it.typ&metricCPU != 0 {
					vals = append(vals, w.formatCPU(v, dark))
				} else { // metricCount
					vals = append(vals, w.formatU64(v, dark, false))
				}
			case metricHist: // metricTime
				count := right[it.name+"_total"] - left[it.name+"_total"]
				var avg float64
				if count > 0.0 {
					cost := right[it.name+"_sum"] - left[it.name+"_sum"]
					if it.typ&metricTime != 0 {
						cost *= 1000 // s -> ms
					}
					avg = cost / count
				}
				if !dark {
					count /= float64(w.interval)
				}
				vals = append(vals, w.formatU64(count, dark, false), w.formatTime(avg, dark))
			}
		}
		values[i] = strings.Join(vals, " ")
	}
	if w.tty && dark {
		fmt.Printf("%s\r", strings.Join(values, w.colorize("|", BLUE, true, false)))
	} else {
		fmt.Printf("%s\n", strings.Join(values, w.colorize("|", BLUE, true, false)))
	}
}

func readStats(path string) map[string]float64 {
	d, err := os.ReadFile(path)
	//if err != nil {
	//	log.Warnf("readStats open %s: %s", path, err)
	//	return nil
	//}
	//defer f.Close()
	//d, err := ioutil.ReadAll(f)
	//if err != nil {
	//	log.Warnf("read %s: %s", path, err)
	//	return nil
	//}
	fmt.Printf("readStats: %s\n", string(d))
	stats := make(map[string]float64)
	lines := strings.Split(string(d), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			stats[fields[0]], err = strconv.ParseFloat(fields[1], 64)
			if err != nil {
				log.Warnf("parse %s: %s", fields[1], err)
			}
		}
	}
	return stats
}

func stats(ctx *cli.Context) error {
	mp := ctx.Args().First()
	inode, err := mountutil.GetFileInode(mp)
	if err != nil {
		log.Fatalf("lookup inode for %s: %s", mp, err)
	}
	if inode != 1 {
		log.Fatalf("path %s is not a mount point", mp)
	}

	watcher := &statsWatcher{
		tty:      isatty.IsTerminal(os.Stdout.Fd()),
		interval: ctx.Uint("interval"),
		path:     path.Join(mp, ".stats"),
	}
	watcher.buildSchema(ctx.String("schema"), ctx.Uint("verbosity"))
	watcher.formatHeader()

	var tick uint
	var start, last, current map[string]float64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	current = readStats(watcher.path)
	start = current
	last = current
	for {

		if tick%(watcher.interval*30) == 0 {
			fmt.Println(watcher.header)
		}
		if tick%watcher.interval == 0 {
			watcher.printDiff(start, current, false)
			start = current
		} else {
			watcher.printDiff(last, current, true)
		}
		last = current
		tick++
		<-ticker.C
		current = readStats(watcher.path)
	}
}
