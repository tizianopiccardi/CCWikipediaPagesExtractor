package main

// Simple linked list to collate efficiently all the markers extracted from a Web page

type PagesList struct {
	head   *ListNode
	tail   *ListNode
	length int32
}

type ListNode struct {
	next *ListNode
	Marker *Page
}

// Appends a Marker element to the referred MarkersList
func (ml *PagesList) append(link *Page) {
	node := ListNode{Marker: link, next: nil}
	if ml.head == nil {
		ml.head = &node
		ml.tail = &node
	} else {
		ml.tail.next = &node
		ml.tail = &node
	}
	ml.length++
}

// Appends a MarkersList to the referred MarkersList
func (ml *PagesList) appendList(linksList *PagesList) {
	if linksList.length > 0 {
		if ml.head == nil {
			ml.head = linksList.head
			ml.tail = linksList.tail
		} else {
			ml.tail.next = linksList.head
			ml.tail = linksList.tail
		}
		ml.length += linksList.length
	}
}

// Creates a copy of the referred MarkersList
func (ml *PagesList) copy() PagesList {
	copied := PagesList{}
	copied.head = ml.head
	copied.tail = ml.tail
	copied.length = ml.length
	return copied
}
