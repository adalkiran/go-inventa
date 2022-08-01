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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v9"
)

type RPCCommandFn func(req *RPCCallRequest) []string

type InventaRole byte

const (
	InventaRoleOrchestrator InventaRole = 1
	InventaRoleService      InventaRole = 2
)

type Inventa struct {
	sync.Mutex

	Ctx                    context.Context
	Client                 *redis.Client
	SelfDescriptor         ServiceDescriptor
	InventaRole            InventaRole
	RPCCommandFnRegistry   map[string]RPCCommandFn
	OrchestratorDescriptor ServiceDescriptor
	IsOrchestratorActive   bool
	IsRegistered           bool

	rpcInternalCommandFnRegistry      map[string]RPCCommandFn
	firstPacketReceivedServiceChannel bool
	rpcRawChannel                     chan string
	rpcRequestChannel                 chan *RPCCallRequest
	rpcResponseChannel                chan *RPCCallResponse

	registeredServices map[ServiceDescriptor]bool

	OnServiceRegistering   func(serviceDescriptor ServiceDescriptor) error
	OnServiceUnregistering func(serviceDescriptor ServiceDescriptor, isZombie bool) error
}

func NewInventa(host string, port int, password string, serviceType string, serviceId string, inventaRole InventaRole, rpcCommandFnRegistry map[string]RPCCommandFn) *Inventa {
	client := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", host, port),
		Password:    password,
		DB:          0, // use default DB
		DialTimeout: 10 * time.Second,
	})

	result := &Inventa{
		Ctx:    context.Background(),
		Client: client,
		SelfDescriptor: ServiceDescriptor{
			ServiceType: serviceType,
			ServiceId:   serviceId,
		},
		InventaRole:          inventaRole,
		RPCCommandFnRegistry: rpcCommandFnRegistry,

		rpcRawChannel:      make(chan string, 2),
		rpcRequestChannel:  make(chan *RPCCallRequest, 2),
		rpcResponseChannel: make(chan *RPCCallResponse, 2),

		registeredServices: map[ServiceDescriptor]bool{},
	}

	result.rpcInternalCommandFnRegistry = map[string]RPCCommandFn{
		"register":           result.rpcInternalCommandRegister,
		"orchestrator-alive": result.rpcInternalCommandOrchestratorAlive,
	}
	return result
}

func (r *Inventa) Start() (context.CancelFunc, error) {
	successPong := false
	lastError := nil
	for i := 1; i <= 10; i++ {
		_, err := r.Client.Ping(r.Ctx).Result()
		if err == nil {
			successPong = true
			break
		}
		lastError = err
		time.Sleep(1 * time.Second)
	}

	if !successPong {
		return nil, fmt.Errorf("cannot connect to redis: %s, error: %s", r.Client.Options().Addr, lastError)
	}

	subCtx, cancelFunc := context.WithCancel(context.Background())

	go r.subscribeInternal(r.SelfDescriptor.GetPubSubChannelName(), r.rpcRawChannel, subCtx)
	go r.run(subCtx)

	for r.firstPacketReceivedServiceChannel {
		select {
		case <-time.After(3 * time.Second):
			break
		}
	}
	r.setSelfActive()

	time.Sleep(100 * time.Millisecond)

	if r.InventaRole == InventaRoleOrchestrator {
		r.DiscoverServices()
	}
	return cancelFunc, nil
}

func (r *Inventa) DiscoverServices() {
	r.Lock()
	defer r.Unlock()
	fmt.Printf("Checking active but unregistered services...\n")
	activeServiceDescriptors, err := r.GetAllActiveServices()
	if err != nil {
		fmt.Printf("Error while processing already registered services on redis: %s\n", err)
	}
	for _, activeServiceDescriptor := range activeServiceDescriptors {
		if r.SelfDescriptor.Encode() == activeServiceDescriptor.Encode() {
			continue
		}
		go r.CallSync(activeServiceDescriptor.Encode(), "orchestrator-alive", []string{r.SelfDescriptor.Encode()}, 3*time.Second)
		if r.OnServiceRegistering(activeServiceDescriptor) == nil {
			r.registeredServices[activeServiceDescriptor] = true
		}
	}
}

func (r *Inventa) subscribeInternal(pubSubChannelName string, messageChan chan<- string, subCtx context.Context) {
	subscription := r.Client.Subscribe(subCtx, pubSubChannelName)
	defer subscription.Close()
	for {
		select {
		case <-subCtx.Done(): // if cancel() execute
			return
		default:
			received, err := subscription.ReceiveTimeout(subCtx, 1000*time.Hour)
			r.firstPacketReceivedServiceChannel = true
			if err != nil {
				continue
			}
			msg, ok := received.(*redis.Message)
			if !ok {
				continue
			}

			messageChan <- msg.Payload
		}
	}
}

func (r *Inventa) run(subCtx context.Context) {
	tickerSetSelfActive := time.NewTicker(3 * time.Second)
	tickerCheckActive := time.NewTicker(4 * time.Second)
	defer tickerSetSelfActive.Stop()
	defer tickerCheckActive.Stop()
	for {
		select {
		case <-subCtx.Done(): // if cancel() execute
			return
		case rawMsg := <-r.rpcRawChannel:
			rawMsgParts := strings.Split(rawMsg, "|")
			rawMsgType := rawMsgParts[0]
			switch rawMsgType {
			case "req":
				req := &RPCCallRequest{}
				req.Decode(rawMsg[len(rawMsgType)+1:])
				r.rpcRequestChannel <- req
			case "resp":
				resp := &RPCCallResponse{}
				resp.Decode(rawMsg[len(rawMsgType)+1:])

				r.rpcResponseChannel <- resp
			}
		case req := <-r.rpcRequestChannel:
			var rpcCommandFn RPCCommandFn
			var ok bool
			rpcCommandFn, ok = r.rpcInternalCommandFnRegistry[req.Method]
			if !ok {
				rpcCommandFn, ok = r.RPCCommandFnRegistry[req.Method]
			}
			var cmdResult []string
			if !ok {
				fmt.Printf("Unknown command type: %s\n", req.Method)
				cmdResult = req.ErrorResponse(fmt.Errorf("unknown command type: %s", req.Method))
			} else {
				cmdResult = rpcCommandFn(req)
			}
			resp := r.newRPCCallResponse(req, cmdResult)
			r.Client.Publish(r.Ctx, "ch:"+req.FromService.Encode(), resp.Encode())
		case <-tickerSetSelfActive.C:
			r.setSelfActive()
		case <-tickerCheckActive.C:
			r.checkRegisteredServices()
		}
	}
}

func (r *Inventa) CallSync(serviceChannel string, method string, args []string, timeout time.Duration) ([]string, error) {
	req := r.newRPCCallRequest(method, args)
	r.Client.Publish(r.Ctx, "ch:"+serviceChannel, req.Encode())
	for {
		select {
		case resp := <-r.rpcResponseChannel:
			if resp.CallId == req.CallId {
				if resp.Data[0] == "error" {
					if len(resp.Data) > 1 {
						return nil, fmt.Errorf(resp.Data[1])
					}
					return nil, fmt.Errorf("undescribed error")
				}
				return resp.Data, nil
			}
		case <-time.After(timeout):
			return nil, fmt.Errorf("sync call of \"%s\" on \"%s\" timed out for %v", method, serviceChannel, timeout)
		}
	}
}

func (r *Inventa) GetAllActiveServices() ([]ServiceDescriptor, error) {
	result := make([]ServiceDescriptor, 0)
	//See: https://redis.uptrace.dev/guide/get-all-keys.html#iterating-over-keys
	iter := r.Client.Scan(r.Ctx, 0, "svc:*", 0).Iterator()
	for iter.Next(r.Ctx) {
		svcDescriptor := ServiceDescriptor{}
		err := svcDescriptor.Decode(iter.Val())
		if err != nil {
			fmt.Printf("Error while decoding service name \"%s\", ignoring: %s\n", iter.Val(), err)
			continue
		}
		result = append(result, svcDescriptor)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Inventa) setSelfActive() {
	if r.InventaRole != InventaRoleOrchestrator && !r.IsRegistered {
		return
	}
	err := r.Client.SetEx(r.Ctx, r.SelfDescriptor.Encode(), 1, 5*time.Second).Err()
	if err != nil {
		fmt.Printf("cannot set service status on redis\n")
	}
}

func (r *Inventa) checkRegisteredServices() {
	r.Lock()
	defer r.Unlock()
	switch r.InventaRole {
	case InventaRoleOrchestrator:
		for serviceDescriptor := range r.registeredServices {
			if !r.IsServiceActive(serviceDescriptor.Encode()) {
				if r.OnServiceUnregistering == nil {
					err := fmt.Errorf("the orchestrator service \"%s\" has not implemented \"OnServiceUnregistering\" event function", r.SelfDescriptor.Encode())
					fmt.Printf("Error on checkRegisteredServices: %s\n", err)
					continue
				} else {
					r.OnServiceUnregistering(serviceDescriptor, true)
				}
				delete(r.registeredServices, serviceDescriptor)
			}
		}
	case InventaRoleService:
		if !r.IsServiceActive(r.OrchestratorDescriptor.Encode()) {
			if r.IsOrchestratorActive {
				fmt.Printf("Error: The orchestrator service %s is not alive anymore.\n", r.OrchestratorDescriptor.Encode())
			}
			r.IsOrchestratorActive = false
		} else if !r.IsOrchestratorActive {
			r.IsOrchestratorActive = true
			fmt.Printf("The orchestrator service %s is alive again.\n", r.OrchestratorDescriptor.Encode())
		}
	}
}

func (r *Inventa) IsServiceActive(serviceFullId string) bool {
	_, err := r.Client.Get(r.Ctx, serviceFullId).Result()
	return err == nil
}

func (r *Inventa) TryRegisterToOrchestrator(orchestratorFullId string, tryCount int, timeout time.Duration) error {
	orchestratorDescriptor, err := ParseServiceFullId(orchestratorFullId)
	if err != nil {
		return err
	}
	var lastErr error
	if tryCount < 1 {
		tryCount = 1
	}
	for i := 1; i <= tryCount; i++ {
		fmt.Printf("Trying to register to %s...\n", orchestratorFullId)
		_, err := r.CallSync(orchestratorFullId, "register", []string{r.SelfDescriptor.Encode()}, timeout)
		if err != nil {
			lastErr = err
			fmt.Printf("Error while registering: %s. Remaining try count: %d\n", err, tryCount-i)
			continue
		}
		r.OrchestratorDescriptor = orchestratorDescriptor
		r.IsRegistered = true
		r.IsOrchestratorActive = true
		r.setSelfActive()
		return nil
	}
	return lastErr
}

func (r *Inventa) rpcInternalCommandRegister(req *RPCCallRequest) []string {
	r.Lock()
	defer r.Unlock()
	serviceDescriptor, err := ParseServiceFullId(req.Args[0])
	if err != nil {
		return req.ErrorResponse(err)
	}
	if r.SelfDescriptor.Encode() == serviceDescriptor.Encode() {
		return []string{"ignored-self"}
	}
	if r.OnServiceRegistering == nil {
		err := fmt.Errorf("the orchestrator service \"%s\" has not implemented \"OnServiceRegistering\" event function", r.SelfDescriptor.Encode())
		fmt.Printf("Error on rpcInternalCommandRegister: %s\n", err)
		return req.ErrorResponse(err)
	}
	err = r.OnServiceRegistering(serviceDescriptor)
	if err != nil {
		return req.ErrorResponse(err)
	}
	r.registeredServices[serviceDescriptor] = true
	return []string{"registered"}
}

func (r *Inventa) rpcInternalCommandOrchestratorAlive(req *RPCCallRequest) []string {
	if r.InventaRole != InventaRoleService {
		return []string{"ignored-not-service"}
	}
	r.Lock()
	defer r.Unlock()
	orchestratorDescriptor, err := ParseServiceFullId(req.Args[0])
	if err != nil {
		return req.ErrorResponse(err)
	}
	if orchestratorDescriptor.Encode() != r.OrchestratorDescriptor.Encode() {
		return []string{"ignored-unknown-source"}
	}
	r.IsOrchestratorActive = true
	fmt.Printf("The orchestrator service %s is alive again.\n", r.OrchestratorDescriptor.Encode())
	return []string{"ok"}
}
