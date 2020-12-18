package gsh

import "strings"

// PascalToUnderline XxxYyy to xxx_yyy
func PascalToUnderline(s string) string {
	tmp := []byte{}
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			tmp = append(tmp, '_')
		}
		if d != '_' {
			j = true
		}
		tmp = append(tmp, d)
	}
	return strings.ToLower(string(tmp[:]))
}

// UnderlineToPascal xxx_yyy to XxxYyy
func UnderlineToPascal(s string) string {
	tmp := []byte{}
	bytes := []byte(s)
	length := len(bytes)
	nextLetterNeedToUpper := true
	for i := 0; i < length; i++ {
		if bytes[i] == '_' {
			nextLetterNeedToUpper = true
			continue
		}
		if nextLetterNeedToUpper && bytes[i] >= 'a' && bytes[i] <= 'z' {
			tmp = append(tmp, bytes[i]-32)
		} else {
			tmp = append(tmp, bytes[i])
		}
		nextLetterNeedToUpper = false
	}
	return string(tmp[:])
}
