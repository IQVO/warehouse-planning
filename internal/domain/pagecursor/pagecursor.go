// Package pagecursor holds the keyset position a list page continues after.
// It is a plain value: encoding it as an opaque `cursor` is an inbound
// adapter concern.
package pagecursor

// Cursor is the last returned row's sort key and id. Repositories order by
// (Sort, ID) so ties never skip or repeat a row.
type Cursor struct {
	Sort string
	ID   string
}
