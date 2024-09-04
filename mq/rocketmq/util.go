package rocketmq

import (
	"fmt"
	"net"
	"strings"
)

func ResolveDomainNames(addresses []string) []string {
	target := make([]string, 0)
	for _, a := range addresses {
		if len(a) == 0 {
			continue
		}
		if strings.HasPrefix(a, "https://") {
			a = a[8:]
		}
		if strings.HasPrefix(a, "http://") {
			a = a[7:]
		}
		hostAndPort := strings.Split(a, ":")
		if len(hostAndPort) == 2 {
			addr, err := net.ResolveIPAddr("ip", hostAndPort[0])
			if err != nil {
				fmt.Println("ResolveIPAddr 解析错误:", err.Error())
				continue
			}
			target = append(target, fmt.Sprintf("%s:%s", addr, hostAndPort[1]))
		} else {
			addr, err := net.ResolveIPAddr("ip", hostAndPort[0])
			if err != nil {
				fmt.Println("ResolveIPAddr 解析错误:", err.Error())
				continue
			}
			target = append(target, fmt.Sprintf("%s:%s", addr, "80"))
		}
	}
	return target
}
