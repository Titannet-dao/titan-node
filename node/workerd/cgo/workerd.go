package cgo

import (
	"encoding/json"
	"github.com/pkg/errors"
)

type JsonCallRequest struct {
	Method string      `json:"method"`
	Args   interface{} `json:"args"`
}

type JsonCallResponse struct {
	Code  int32  `json:"code"`
	Msg   string `json:"msg"`
	State int    `json:"state"`
	Error string `json:"error"`
}

func jsonCall(jsonString string) (*JsonCallResponse, error) {
	result, err := callC(jsonString)
	if err != nil {
		return nil, err
	}

	var resp JsonCallResponse
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func InitWorkerdRuntime() error {
	param := map[string]string{
		"reportURL": "",
	}

	marshalledParam, err := json.Marshal(param)
	if err != nil {
		return err
	}

	request := JsonCallRequest{
		Method: "initRuntime",
		Args:   string(marshalledParam),
	}

	marshalledJsonCallParam, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := jsonCall(string(marshalledJsonCallParam))
	if err != nil {
		return nil
	}

	if resp.Code != 0 {
		return errors.Errorf("jsonCall: %s", resp.Msg)
	}

	return nil
}

func CreateWorkerd(projectId string, directory, configFilePath, addr string) error {
	param := map[string]string{
		"id":         projectId,
		"directory":  directory,
		"configFile": configFilePath,
		"socketAddr": addr,
	}

	marshalledParam, err := json.Marshal(param)
	if err != nil {
		return err
	}

	request := JsonCallRequest{
		Method: "createWorkerd",
		Args:   string(marshalledParam),
	}

	marshalledJsonCallParam, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := jsonCall(string(marshalledJsonCallParam))
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		return errors.Errorf("jsonCall: %s", resp.Msg)
	}

	return nil
}

func DestroyWorkerd(projectId string) error {
	param := map[string]string{
		"id": projectId,
	}

	marshalledParam, err := json.Marshal(param)
	if err != nil {
		return err
	}

	jsonCallParam := JsonCallRequest{
		Method: "destroyWorkerd",
		Args:   string(marshalledParam),
	}

	marshalledJsonCallParam, err := json.Marshal(jsonCallParam)
	if err != nil {
		return err
	}

	resp, err := jsonCall(string(marshalledJsonCallParam))
	if err != nil {
		return nil
	}

	if resp.Code != 0 {
		return errors.Errorf("jsonCall: %s", resp.Msg)
	}

	return nil
}

func QueryWorkerd(projectId string) (bool, error) {
	param := map[string]string{
		"id": projectId,
	}

	marshalledParam, err := json.Marshal(param)
	if err != nil {
		return false, err
	}

	jsonCallParam := JsonCallRequest{
		Method: "queryWorkerd",
		Args:   string(marshalledParam),
	}

	marshalledJsonCallParam, err := json.Marshal(jsonCallParam)
	if err != nil {
		return false, err
	}

	resp, err := jsonCall(string(marshalledJsonCallParam))
	if err != nil {
		return false, nil
	}

	if resp.Code != 0 {
		return false, errors.Errorf("jsonCall: %s", resp.Msg)
	}

	if resp.State != 0 {
		return false, errors.Errorf(resp.Error)
	}

	return true, nil
}
