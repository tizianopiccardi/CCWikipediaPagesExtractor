package main

import "unicode/utf8"

type Page struct {
	Url     string `parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Content       string `parquet:"name=content, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}


func NewPage(
	url string,
	content string,
) Page {

	return Page{
		Url:     toValidUTF8(url),
		Content:       toValidUTF8(content),
	}
}

func toValidUTF8(text string) string {
	if utf8.ValidString(text) {
		return text
	}
	s := []byte(text)
	b := make([]byte, 0, len(s))
	invalid := false // previous byte was from an invalid UTF-8 sequence
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			invalid = false
			b = append(b, byte(c))
			continue
		}
		_, wid := utf8.DecodeRune(s[i:])
		if wid == 1 {
			i++
			if !invalid {
				invalid = true
			}
			continue
		}
		invalid = false
		b = append(b, s[i:i+wid]...)
		i += wid
	}
	return string(b)
}
