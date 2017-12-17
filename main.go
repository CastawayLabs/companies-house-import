package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // imports mysql driver
)

func main() {
	dbUser := flag.String("user", "", "Database user")
	dbPass := flag.String("password", "", "Database password")
	dbName := flag.String("db", "", "Database name")
	dbHost := flag.String("host", "", "Database host")
	url := flag.String("url", "", "Download specific data")
	flag.Parse()

	if url == nil || len(*url) == 0 {
		yesterday := time.Now().Add(-time.Hour * 24).Format("2006-01-02")
		*url = "http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-" + yesterday + ".zip"
	}

	fmt.Println(url)

	resp, err := http.Get(*url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Println(resp.StatusCode)
		return
	}

	out, _ := os.Create("/tmp/company_data.zip")
	defer out.Close()

	if _, err = io.Copy(out, resp.Body); err != nil {
		panic(err)
	}

	fmt.Println("unzipping..")

	zipped, err := zip.OpenReader("/tmp/company_data.zip")
	if err != nil {
		panic(err)
	}

	f, _ := zipped.File[0].Open()
	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	// read header
	reader.Read()

	fmt.Println("reading..")

	db, err := sql.Open("mysql", strings.Join([]string{*dbUser, ":", *dbPass, "@tcp(", *dbHost, ")/", *dbName, "?parseTime=true&interpolateParams=true"}, ""))
	if err != nil {
		panic(err)
	}

	if _, err := db.Exec("CREATE TABLE new_companies LIKE companies"); err != nil {
		panic(err)
	}
	if _, err := db.Exec("RENAME TABLE companies TO old_companies, new_companies TO companies"); err != nil {
		panic(err)
	}
	if _, err := db.Exec("DROP TABLE old_companies"); err != nil {
		panic(err)
	}

	db.Exec(`CREATE TABLE IF NOT EXISTS data_import (
		created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
		records int(10) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8`)

	i := 0
	dateColumns := []int{13, 14, 17, 18, 20, 21, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 54}
	intColumns := []int{15, 16, 22, 23, 24, 25, 30, 31}
	for {
		i++
		columns, err := reader.Read()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err == io.EOF {
			break
		}

		if i%1000 == 0 {
			fmt.Printf("Progress: %d\n", i)
		}

		args := []interface{}{}
		for col, column := range columns {
			foundDate := false
			foundInt := false

			for _, dateColumn := range dateColumns {
				foundDate = dateColumn == col

				if foundDate {
					break
				}
			}
			for _, intColumn := range intColumns {
				foundInt = intColumn == col

				if foundInt {
					break
				}
			}

			if foundDate {
				date, err := time.Parse("02/01/2006", column)
				if err == nil {
					args = append(args, date)
				} else {
					args = append(args, nil)
				}
			} else if foundInt {
				i, err := strconv.ParseInt(column, 10, 32)
				if err == nil {
					args = append(args, i)
				} else {
					args = append(args, nil)
				}
			} else {
				args = append(args, column)
			}
		}

		if _, err := db.Exec("INSERT INTO companies (CompanyName, CompanyNumber, RegAddress_CareOf, RegAddress_POBox, RegAddress_AddressLine1, RegAddress_AddressLine2, RegAddress_PostTown, RegAddress_County, RegAddress_Country, RegAddress_PostCode, CompanyCategory, CompanyStatus, CountryOfOrigin, DissolutionDate, IncorporationDate, Accounts_AccountRefDay, Accounts_AccountRefMonth, Accounts_NextDueDate, Accounts_LastMadeUpDate, Accounts_AccountCategory, Returns_NextDueDate, Returns_LastMadeUpDate, Mortgages_NumMortCharges, Mortgages_NumMortOutstanding, Mortgages_NumMortPartSatisfied, Mortgages_NumMortSatisfied, SICCode_SicText_1, SICCode_SicText_2, SICCode_SicText_3, SICCode_SicText_4, LimitedPartnerships_NumGenPartners, LimitedPartnerships_NumLimPartners, URI, PreviousName_1_CONDATE, PreviousName_1_CompanyName, PreviousName_2_CONDATE, PreviousName_2_CompanyName, PreviousName_3_CONDATE, PreviousName_3_CompanyName, PreviousName_4_CONDATE, PreviousName_4_CompanyName, PreviousName_5_CONDATE, PreviousName_5_CompanyName, PreviousName_6_CONDATE, PreviousName_6_CompanyName, PreviousName_7_CONDATE, PreviousName_7_CompanyName, PreviousName_8_CONDATE, PreviousName_8_CompanyName, PreviousName_9_CONDATE, PreviousName_9_CompanyName, PreviousName_10_CONDATE, PreviousName_10_CompanyName, ConfStmtNextDueDate, ConfStmtLastMadeUpDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", args...); err != nil {
			panic(err)
		}
	}

	if _, err := db.Exec(`insert into data_import set created=NOW(), records=?`, i); err != nil {
		panic(err)
	}

	// cleanup
	f.Close()
	os.Remove("/tmp/company_data.csv")

	return
}
