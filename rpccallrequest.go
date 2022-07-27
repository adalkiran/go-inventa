/*
   Copyright (c) 2022-present, Adil Alper DALKIRAN

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

package inventa

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type RPCCallRequest struct {
	CallId      string
	FromService ServiceDescriptor
	Method      string
	Args        []string
}

func (r *Inventa) newRPCCallRequest(method string, args []string) *RPCCallRequest {
	return &RPCCallRequest{
		CallId:      randStringRunes(5) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		FromService: r.SelfDescriptor,
		Method:      method,
		Args:        args,
	}
}

func (r *RPCCallRequest) Encode() string {
	return "req|" + r.CallId + "|" + r.FromService.Encode() + "|" + r.Method + "|" + encodeContentArray(r.Args)
}

func (r *RPCCallRequest) Decode(raw string) error {
	rawParts := strings.Split(raw, "|")
	r.CallId = rawParts[0]
	r.FromService, _ = ParseServiceFullId(rawParts[1])
	r.Method = rawParts[2]
	r.Args = decodeContentArray(rawParts[3])
	return nil
}

func (r *RPCCallRequest) ErrorResponse(err error) []string {
	return []string{"error", fmt.Sprintf("%s", err)}
}
