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
	"strings"
)

type ServiceDescriptor struct {
	ServiceType string
	ServiceId   string
}

func (d *ServiceDescriptor) Encode() string {
	return "svc:" + d.ServiceType + ":" + d.ServiceId
}

func (d *ServiceDescriptor) Decode(serviceFullId string) error {
	rawParts := strings.Split(serviceFullId, ":")
	if len(rawParts) != 3 || rawParts[0] != "svc" {
		return fmt.Errorf("invalid InventaServiceDescriptor value: %s", serviceFullId)
	}
	d.ServiceType = rawParts[1]
	d.ServiceId = rawParts[2]
	return nil
}

func (d *ServiceDescriptor) GetPubSubChannelName() string {
	return "ch:" + d.Encode()
}

func ParseServiceFullId(serviceFullId string) (ServiceDescriptor, error) {
	result := ServiceDescriptor{}
	err := result.Decode(serviceFullId)
	if err != nil {
		return result, fmt.Errorf("error while decoding service name \"%s\": %s", serviceFullId, err)
	}
	return result, nil
}
