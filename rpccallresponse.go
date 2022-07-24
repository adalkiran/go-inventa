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

import "strings"

type RPCCallResponse struct {
	CallId      string
	FromService ServiceDescriptor
	Data        string
}

func (r *Inventa) newRPCCallResponse(req *RPCCallRequest, data string) *RPCCallResponse {
	return &RPCCallResponse{
		CallId:      req.CallId,
		FromService: r.SelfDescriptor,
		Data:        data,
	}
}

func (r *RPCCallResponse) Encode() string {
	return "resp|" + r.CallId + "|" + r.FromService.Encode() + "|" + r.Data
}

func (r *RPCCallResponse) Decode(raw string) error {
	rawParts := strings.Split(raw, "|")
	r.CallId = rawParts[0]
	r.FromService, _ = ParseServiceFullId(rawParts[1])
	r.Data = strings.Join(rawParts[2:], "|")
	return nil
}
