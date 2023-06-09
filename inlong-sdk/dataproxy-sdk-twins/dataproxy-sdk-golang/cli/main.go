//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/dataproxy"
	"go.uber.org/atomic"
)

var (
	url      string
	groupID  string
	streamID string
	payload  string
	count    int
	addCols  mapFlag
	async    bool
	succeed  atomic.Int32
	failed   atomic.Int32
)

type mapFlag map[string]string

func (f mapFlag) String() string {
	return fmt.Sprintf("%v", map[string]string(f))
}

func (f mapFlag) Set(value string) error {
	split := strings.SplitN(value, "=", 2)
	if len(split) < 2 {
		return errors.New("invalid map flag")
	}

	f[split[0]] = split[1]
	return nil
}

func main() {
	addCols = make(map[string]string)
	flag.StringVar(&url, "url", "http://127.0.0.1:8083/inlong/manager/openapi/dataproxy/getIpList", "the Manager URL")
	flag.StringVar(&groupID, "group-id", "test_pusar_group", "Group ID")
	flag.StringVar(&streamID, "stream-id", "test_pusar_stream", "Stream ID")
	flag.StringVar(&payload, "payload", "sdk_test_1|1", "message payload")
	flag.IntVar(&count, "count", 10, "send count")
	flag.Var(&addCols, "col", "add columns, for example: -col k1=v1 -col k2=v2")
	flag.BoolVar(&async, "async", false, "send asynchronously")
	flag.Parse()

	var err error
	client, err := dataproxy.NewClient(
		dataproxy.WithGroupID(groupID),
		dataproxy.WithURL(url),
		dataproxy.WithMetricsName("cli"),
		dataproxy.WithAddColumns(addCols),
	)

	if err != nil {
		log.Fatal(err)
	}

	msg := dataproxy.Message{GroupID: groupID, StreamID: streamID, Payload: []byte(payload)}
	for i := 0; i < count; i++ {
		if !async {
			err = client.Send(context.Background(), msg)
			if err != nil {
				failed.Add(1)
				fmt.Printf("send msg error=[%v] streamID=[%s] payLoad=[%s]\n", err, msg.StreamID, string(msg.Payload))
			} else {
				succeed.Add(1)
			}
		} else {
			client.SendAsync(context.Background(), msg, onResult)
		}
	}

	if async {
		wait()
	} else {
		fmt.Printf("send msg finish. succeed=%d failed=%d\n", succeed.Load(), failed.Load())
	}
}

func onResult(msg dataproxy.Message, err error) {
	if err != nil {
		fmt.Printf("async send msg error=[%v] streamID=[%s] payLoad=[%s]\n", err, msg.StreamID, string(msg.Payload))
		failed.Add(1)
	} else {
		succeed.Add(1)
	}
}

func wait() {
	maxWaitTicker := time.NewTicker(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			fmt.Printf("async sending msg. succeed=%d failed=%d\n", succeed.Load(), failed.Load())
			finished := int(failed.Load() + succeed.Load())
			if finished >= count {
				return
			}
		case <-maxWaitTicker.C:
			fmt.Println("async send msg timeout")
			return
		}
	}
}
