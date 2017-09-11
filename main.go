package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	_ "gopkg.in/goracle.v2"
)

var replOwner = strings.NewReplacer("'", "''", "\n", " ")

func main() {
	flagConnect := flag.String("connect", os.Getenv("BRUNO_ID"), "database to connect to")
	flagJSON := flag.String("json", "", "json file to read/write from")
	flag.Parse()

	var db *sql.DB
	if *flagConnect != "" || *flagJSON == "" {
		var err error
		db, err = sql.Open("goracle", *flagConnect)
		if err != nil {
			log.Fatal(errors.Wrap(err, *flagConnect))
		}
		defer db.Close()
	}
	var buf bytes.Buffer

	var ownerW string
	owners := flag.Args()
	if len(owners) != 0 {
		buf.WriteString("AND A.owner IN (")
		for i, owner := range owners {
			if i != 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('\'')
			buf.WriteString(replOwner.Replace(owner))
			buf.WriteByte('\'')
		}
		buf.WriteString(")")
		ownerW = buf.String()
	}

	if err := Main(os.Stdout, db, *flagJSON, ownerW); err != nil {
		log.Fatalf("%+v", err)
	}
}

const Dot = "__"

var replDot = strings.NewReplacer(".", Dot, "$", "_")

func name(s ...string) string { return replDot.Replace(strings.Join(s, Dot)) }

func Main(w io.Writer, db *sql.DB, jsonFile string, ownerW string) error {
	var constraints map[string][]Constraint
	var tables []Table
	if db != nil {
		var grp errgroup.Group
		grp.Go(func() error {
			var err error
			constraints, err = readConstraints(db, ownerW)
			return err
		})

		grp.Go(func() error {
			var err error
			tables, err = readTables(db, ownerW)
			return err
		})
		if err := grp.Wait(); err != nil {
			return err
		}
		if jsonFile != "" {
			fh, err := os.Create(jsonFile)
			if err != nil {
				return err
			}
			defer fh.Close()
			enc := json.NewEncoder(fh)
			if err := enc.Encode(constraints); err != nil {
				return err
			}
			if err := enc.Encode(tables); err != nil {
				return err
			}
			if err := fh.Close(); err != nil {
				return err
			}
		}
	} else {
		fh, err := os.Open(jsonFile)
		if err != nil {
			return err
		}
		defer fh.Close()
		dec := json.NewDecoder(fh)
		if err := dec.Decode(&constraints); err != nil {
			return err
		}
		if err := dec.Decode(&tables); err != nil {
			return err
		}
	}

	bw := bufio.NewWriter(w)
	defer bw.Flush()

	bw.WriteString("digraph {\n")
	for _, t := range tables {
		fmt.Fprintf(bw, "  %s [shape=plain label=<<TABLE TITLE=%q><TR><TD><B>%s.%s</B></TD></TR>\n",
			name(t.Owner, t.Name), url.QueryEscape(t.Comment), t.Owner, t.Name)
		for _, col := range t.Columns {
			fmt.Fprintf(bw, "<TR><TD TITLE=%q>%s %s</TD></TR>\n", url.QueryEscape(col.Comment), col.Name, col.Type)
		}
		bw.WriteString("</TABLE>>];\n")
	}
	for tbl, vv := range constraints {
		for _, v := range vv {
			fmt.Fprintf(bw, "  %s -> %s [label=%s];\n", name(tbl), name(v.Owner, v.Table), v.Type)
		}
	}
	bw.WriteString("\n}\n")
	return bw.Flush()
}

func readTables(db *sql.DB, ownerW string) ([]Table, error) {
	qryCols := `SELECT A.owner, A.table_name, A.column_name,
  (CASE A.data_type
     WHEN 'NUMBER' THEN A.data_type||'('||A.data_precision||','||A.data_scale||')'
     WHEN 'DATE' THEN A.data_type
     ELSE A.data_type||'('||A.data_length||')'
     END)||(CASE A.nullable WHEN 'N' THEN ' NOT NULL' END) data_type,
     B.comments tab_comment,
     C.comments col_comment
  FROM all_col_comments C, all_tab_comments B, all_tab_cols A
  WHERE C.column_name = A.column_name AND C.owner = A.owner AND C.table_name = A.table_name AND
        B.owner = A.owner AND B.table_name = A.table_name
        ` + ownerW + `
  ORDER BY A.owner, A.table_name, A.column_id`

	rows, err := db.Query(qryCols)
	if err != nil {
		return nil, errors.Wrap(err, qryCols)
	}
	defer rows.Close()

	var tables []Table
	var tp Table
	for rows.Next() {
		var c Column
		var ta Table
		if err := rows.Scan(&ta.Owner, &ta.Name, &c.Name, &c.Type, &ta.Comment, &c.Comment); err != nil {
			return tables, err
		}
		ta.Comment = strings.TrimSpace(ta.Comment)
		c.Comment = strings.TrimSpace(c.Comment)
		if tp.Name == "" {
			tp = ta
			continue
		}
		if !(tp.Owner == ta.Owner && tp.Name == ta.Name) {
			tables = append(tables, tp)
			tp = ta
			continue
		}
		tp.Columns = append(tp.Columns, c)
	}
	if tp.Name != "" {
		tables = append(tables, tp)
	}
	return tables, nil
}

type Table struct {
	Owner, Name, Comment string
	Columns              []Column
}
type Column struct {
	Name, Type, Comment string
}

type Constraint struct {
	Owner, Name, Type, Table string
	Columns                  []string
}

func readConstraints(db *sql.DB, ownerW string) (map[string][]Constraint, error) {
	qryCons := `SELECT A.owner, A.constraint_name, a.constraint_type,
       B.table_name, B.column_name
  FROM all_cons_columns B, all_constraints A
  WHERE A.constraint_type IN ('R', 'P', 'U') AND
        B.owner = a.owner AND B.constraint_name = A.constraint_name
        ` + ownerW + `
  ORDER BY A.owner, A.constraint_name`

	rows, err := db.Query(qryCons)
	if err != nil {
		return nil, errors.Wrap(err, qryCons)
	}
	defer rows.Close()
	constraints := make(map[string][]Constraint)
	var cp Constraint
	for rows.Next() {
		var colName string
		var ca Constraint
		if err := rows.Scan(&ca.Owner, &ca.Name, &ca.Type, &ca.Table, &colName); err != nil {
			return constraints, err
		}
		if cp.Name == "" {
			cp = ca
			continue
		}
		if !(ca.Owner == cp.Owner && ca.Name == cp.Name && ca.Type == cp.Type && ca.Table == cp.Table) {
			k := cp.Owner + "." + cp.Table
			constraints[k] = append(constraints[k], cp)
			cp = ca
			continue
		}
		cp.Columns = append(cp.Columns, colName)
	}
	if cp.Name != "" {
		k := cp.Owner + "." + cp.Table
		constraints[k] = append(constraints[k], cp)
	}
	return constraints, nil
}
