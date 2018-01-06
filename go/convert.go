package main

import (
	"encoding/json"
	"fmt"
	"strconv"
)

func main() {

	// this is what your input string looks like...
	qs := "{\\x22device_id\\x22: \\x221234567890\\x22}"

	// now let's convert it to a normal string
	// note that it has to look like a Go string literal so we're
	// using Sprintf
	s, err := strconv.Unquote(fmt.Sprintf(`"%s"`, qs))
	if err != nil {
		panic(err)
	}
	fmt.Println(s)

	// just for good measure, let's see if it can actually be decoded.
	// SPOILER ALERT: It decodes just fine!
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		panic(err)

	}
	fmt.Println(v)
}
