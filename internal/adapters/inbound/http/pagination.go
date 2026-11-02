package http

import (
	"net/http"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/adapters/inbound/http/page"
)

// pageParams reads the paging parameters of a collection GET (fleet REST API
// guideline section 3/4.2): a query parameter the collection does not declare
// is 400 unknown-query-parameter, a bad limit 400 invalid-limit, an
// undecodable cursor 400 invalid-cursor. filters lists the declared filters.
// On success it returns the page size, the cursor to continue after (nil =
// first page) and the number of rows to fetch (limit+1, to detect a next page).
func pageParams(w http.ResponseWriter, r *http.Request, filters ...string) (limit int, after *pagecursor.Cursor, ok bool) {
	if name := page.Unknown(r, filters...); name != "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"unknown-query-parameter", "Unknown query parameter"}, "query parameter \""+name+"\" is not declared by this collection (paging uses limit and cursor)", r.URL.Path)
		return 0, nil, false
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-limit", "limit must be an integer between 1 and 500"}, err.Error(), r.URL.Path)
		return 0, nil, false
	}
	pos, err := page.After(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-cursor", "cursor is not a valid page token"}, err.Error(), r.URL.Path)
		return 0, nil, false
	}
	if pos != nil {
		after = &pagecursor.Cursor{Sort: pos.Sort, ID: pos.ID}
	}
	return limit, after, true
}
