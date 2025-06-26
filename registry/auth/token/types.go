package token

import (
	"encoding/json"
	"errors"
)

const maxAudienceListLength = 10

// AudienceList is a slice of strings that can be deserialized from either a single string value or a list of strings.
type AudienceList []string

var errInvalidAudience = errors.New("invalid audience value")

func (s *AudienceList) UnmarshalJSON(data []byte) (err error) {
	var value any

	if err = json.Unmarshal(data, &value); err != nil {
		return err
	}

	switch v := value.(type) {
	case string:
		*s = []string{v}

	case []string:
		*s = v

	case []any:
		if len(v) == 0 {
			return nil
		}
		if len(v) > maxAudienceListLength {
			return errInvalidAudience
		}
		ss := make([]string, len(v))

		for i, vv := range v {
			vs, ok := vv.(string)
			if !ok {
				return errInvalidAudience
			}

			ss[i] = vs
		}

		*s = ss

	case nil:
		return nil

	default:
		return errInvalidAudience
	}

	return nil
}

func (s AudienceList) MarshalJSON() (b []byte, err error) {
	return json.Marshal([]string(s))
}
