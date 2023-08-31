package utils

import (
	"log"
	"strconv"
	"strings"
)

func ParseReviewerIDs(input string) []int {
	idStrings := strings.Split(input, ",")
	reviewerIDs := make([]int, 0, len(idStrings))

	for _, idString := range idStrings {
		id, err := strconv.Atoi(strings.TrimSpace(idString))
		if err != nil {
			log.Printf("Failed to assign reviwers to the MR: %v", err)
			return nil
		}
		reviewerIDs = append(reviewerIDs, id)
	}

	return reviewerIDs
}
