package driver

import (
	"net/url"
	"strconv"
)

// CustomParams generates a url.Values from a list of `opts` that correspond to `customParamKeys` or nil if none was set.
func CustomParams(options map[string]any, customParamMap map[string]string) url.Values {
	urlParams := url.Values{}
	for lookupKey, paramKey := range customParamMap {
		if paramVal, ok := options[lookupKey]; ok && paramVal != nil {
			switch v := paramVal.(type) {
			case string:
				urlParams.Set(paramKey, v)
			case int64:
				urlParams.Set(paramKey, strconv.FormatInt(v, 10))
			}
		}
	}

	if len(urlParams) == 0 {
		return nil
	}

	return urlParams
}
