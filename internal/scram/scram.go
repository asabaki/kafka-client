// Package scram adapts github.com/xdg-go/scram to sarama's SCRAMClient interface.
package scram

import (
	"github.com/xdg-go/scram"
)

// Client is a sarama.SCRAMClient for the hash function in HashGeneratorFcn.
type Client struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *Client) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *Client) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *Client) Done() bool {
	return x.ClientConversation.Done()
}
