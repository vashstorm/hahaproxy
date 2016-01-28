package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type ConfigType struct {
	Filename string `config file`
	Sess     map[string]map[string]string
}

func NewConfig(filename string) (*ConfigType, error) {
	ff, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer ff.Close()
	ct := new(ConfigType)
	ct.Sess = make(map[string]map[string]string)

	r := bufio.NewReader(ff)

	var key string
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			return ct, nil
		}

		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			kv := strings.Fields(line)

			for i, val := range kv {
				if strings.HasPrefix(val, "#") {
					kv = kv[:i]
					break
				}
			}

			if len(kv) == 0 {
				continue
			}

			if len(kv) != 2 {
				return nil, fmt.Errorf("config file is load Error")
			}

			val, ok := ct.Sess[key][kv[0]]
			if ok { // mutil
				val += " " + kv[1]
				ct.Sess[key][kv[0]] = val
			} else {
				ct.Sess[key][kv[0]] = kv[1]
			}
		} else {
			kv := strings.Fields(line)

			for i, val := range kv {
				if strings.HasPrefix(val, "#") {
					kv = kv[:i]
					break
				}
			}

			if len(kv) == 0 {
				continue
			}

			if kv[0] == "global" {
				key = kv[0]
			} else if kv[0] == "listen" && len(kv) == 2 {
				key = kv[1]
			}

			ct.Sess[key] = make(map[string]string)
		}
	}

	return ct, nil
}

func (ct *ConfigType) Get(key, subkey string) string {
	if ct == nil {
		fmt.Println("conf is nil")
		return ""
	}

	if submap, ok := ct.Sess[key]; ok {
		if val, ok := submap[subkey]; ok {
			return val
		}
	}

	return ""
}

func (ct *ConfigType) AllKey() []string {
	var res []string
	for kk, _ := range ct.Sess {
		res = append(res, kk)
	}

	return res
}

func (ct *ConfigType) GetSess(key string) map[string]string {
	if submap, ok := ct.Sess[key]; ok {
		return submap
	}

	return nil
}
