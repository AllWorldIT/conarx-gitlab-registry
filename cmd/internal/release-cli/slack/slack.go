package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

type SlackRequestBody struct {
	Text string `json:"text"`
}

func SendSlackNotification(webhookUrl string, msg string) error {
	slackBody, err := json.Marshal(SlackRequestBody{Text: msg})
	if err != nil {
		return err
	}
	resp, err := httpClient.Post(webhookUrl, "application/json", bytes.NewBuffer(slackBody))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		if err != nil {
			return err
		}
		return errors.New("Non-ok response returned from Slack: " + buf.String())
	}

	return nil
}
