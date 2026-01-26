package utils

import (
	"log"
	"strconv"
	"strings"
)

func ParseReviewerIDs(input string) []int64 {
	idStrings := strings.Split(input, ",")
	reviewerIDs := make([]int64, 0, len(idStrings))

	for _, idString := range idStrings {
		id, err := strconv.ParseInt(strings.TrimSpace(idString), 10, 0)
		if err != nil {
			log.Printf("Failed to assign reviwers to the MR: %v", err)
			return nil
		}
		reviewerIDs = append(reviewerIDs, id)
	}

	return reviewerIDs
}
