package driver

import "net/url"

// CustomParams generates a url.Values from a list of `opts` that correspond to `customParamKeys` or nil if none was set.
func CustomParams(options map[string]any, customParamMap map[string]string) url.Values {
	urlParams := url.Values{}
	for key, val := range customParamMap {
		if paramVal, ok := options[key]; ok && paramVal != nil {
			if paramValString, ok := paramVal.(string); ok {
				urlParams.Set(val, paramValString)
			}
		}
	}

	if len(urlParams) == 0 {
		return nil
	}

	return urlParams
}
