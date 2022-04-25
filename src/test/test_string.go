package main

import (
	"fmt"
	"strings"
)

func main() {
	s := "mr-0-1"
	ss := strings.Split(s, "-")
	fmt.Println(ss[len(ss)-1])

}
