package optimizer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
	"golang.org/x/exp/slices"
)

type joinExpression struct {
	LeftTable           string
	LeftTableAliasName  string
	JoinType            string
	RightTable          string
	RightTableAliasName string
	OnCondition         string
	Tables              []string
	Columns             []string
	JoinDependencyList  []string
	Dependencies        []joinExpression
}

type queryInfo struct {
	Alias           string
	TableAliasNames []string
	Tables          []string
	Columns         []string
	Expression      string
	JoinExpression  []joinExpression
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Error has occured")
		panic(err)
	}
}

func reverseSliceOfStruct(joinData []joinExpression) {
	for i, j := 0, len(joinData)-1; i < j; i, j = i+1, j-1 {
		joinData[i], joinData[j] = joinData[j], joinData[i]
	}
}

func reverseSliceofStrings(dependencyList []string) {
	for i, j := 0, len(dependencyList)-1; i < j; i, j = i+1, j-1 {
		dependencyList[i], dependencyList[j] = dependencyList[j], dependencyList[i]
	}
}

func removeDuplicates(columns, tables []string) ([]string, []string) {
	colResult := make(map[string]bool)
	var uniCols []string
	for _, str := range columns {
		if _, ok := colResult[str]; !ok {
			colResult[str] = true
			uniCols = append(uniCols, str)
		}
	}

	tabResult := make(map[string]bool)
	var uniTabs []string
	for _, str := range tables {
		if _, ok := tabResult[str]; !ok {
			tabResult[str] = true
			uniTabs = append(uniTabs, str)
		}
	}
	return uniCols, uniTabs
}

func cleanList(input []string) []string {
	result := make(map[string]bool)
	var cleanedOutput []string
	for _, str := range input {
		if _, ok := result[str]; !ok {
			result[str] = true
			cleanedOutput = append(cleanedOutput, str)
		}
	}

	return cleanedOutput
}

func preprocessing(data string) string {
	data = strings.Replace(data, "@all_account_ids", "('@all_account_ids')", -1)
	data = strings.Replace(data, "@account_id", "'@account_id'", -1)
	data = strings.Replace(data, "@cc_eu_cut_off_date", "'@cc_eu_cut_off_date'", -1)
	data = strings.Replace(data, "@revocation_date_column,", "-- @revocation_date_column,", -1)
	data = strings.Replace(data, "@revocation_date_join_condition_1", "-- @revocation_date_join_condition_1", -1)
	data = strings.Replace(data, "@revocation_date_join_condition_2", "-- @revocation_date_join_condition_2", -1)
	data = strings.Replace(data, "@revocation_date_join_condition_3", "-- @revocation_date_join_condition_3", -1)
	data = strings.Replace(data, "@encryption_everywhere_condition_1", "-- @encryption_everywhere_condition_1", -1)
	data = strings.Replace(data, "@encryption_everywhere_condition_2", "-- @encryption_everywhere_condition_2", -1)
	data = strings.Replace(data, "SUBSTRING", "XYZ", -1)
	return data
}

func finalProcessing(data string) string {
	data = strings.Replace(data, "('@all_account_ids')", "@all_account_ids", -1)
	data = strings.Replace(data, "'@account_id'", "@account_id", -1)
	data = strings.Replace(data, "'@cc_eu_cut_off_date'", "@cc_eu_cut_off_date", -1)
	data = strings.Replace(data, "XYZ", "SUBSTRING", -1)
	return data
}

func extractColumns(expr sqlparser.Expr) ([]string, []string) {
	var columns []string
	var tables []string

	if c, ok := expr.(*sqlparser.ComparisonExpr); ok {
		if col, ok := c.Left.(*sqlparser.ColName); ok {
			columns = append(columns, col.Name.String())
			tables = append(tables, col.Qualifier.Name.String())
		} else {
			fmt.Printf("inner left else for %s\n", sqlparser.String(c))
		}
		if col, ok := c.Right.(*sqlparser.ColName); ok {
			columns = append(columns, col.Name.String())
			tables = append(tables, col.Qualifier.Name.String())
		} else if selExpr, ok := c.Right.(*sqlparser.Subquery); ok {
			nestedSelect, _ := selExpr.Select.(*sqlparser.Select)
			for _, tableExpr := range nestedSelect.From {
				if at, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
					tables = append(tables, sqlparser.String(at.Expr))
				}
			}
		}
	} else if andExpr, ok := expr.(*sqlparser.AndExpr); ok {
		cols, tabs := extractColumns(andExpr.Left)
		columns, tables = append(columns, cols...), append(tables, tabs...)
		cols, tabs = extractColumns(andExpr.Right)
		columns, tables = append(columns, cols...), append(tables, tabs...)
	} else if orExpr, ok := expr.(*sqlparser.OrExpr); ok {
		cols, tabs := extractColumns(orExpr.Left)
		columns, tables = append(columns, cols...), append(tables, tabs...)
		cols, tabs = extractColumns(orExpr.Right)
		columns, tables = append(columns, cols...), append(tables, tabs...)
	} else if isExpr, ok := expr.(*sqlparser.IsExpr); ok {
		if col, ok := isExpr.Expr.(*sqlparser.ColName); ok {
			columns = append(columns, col.Name.String())
			tables = append(tables, col.Qualifier.Name.String())
		}
	} else if paraExpr, ok := expr.(*sqlparser.ParenExpr); ok {
		cols, tabs := extractColumns(paraExpr.Expr)
		columns, tables = append(columns, cols...), append(tables, tabs...)
	}
	return columns, tables
}

func caseHandler(casePart *sqlparser.CaseExpr) ([]string, []string) {
	// fmt.Printf("Case expression %s \n", sqlparser.String(casePart))
	var columns []string
	var tables []string
	_ = tables
	_ = columns
	// fmt.Println("Case Expression detected")
	// fmt.Printf("\tWhen expressions: \n")
	for _, when := range casePart.Whens {
		var cols []string
		_ = cols
		var tabs []string
		_ = tabs
		// fmt.Printf("\tCondition %v, Result %v\n", sqlparser.String(when.Cond), sqlparser.String(when.Val))

		if _, ok := when.Cond.(*sqlparser.ParenExpr); ok {
			when.Cond = when.Cond.(*sqlparser.ParenExpr).Expr
		}

		switch whenExpr := when.Cond.(type) {
		case *sqlparser.AndExpr:
			cols, tabs = extractColumns(whenExpr)
		case *sqlparser.OrExpr:
			cols, tabs = extractColumns(whenExpr)
		case *sqlparser.BinaryExpr:
			cols, tabs = binaryHandler(whenExpr)
		case *sqlparser.ComparisonExpr:
			cols, tabs = extractColumns(whenExpr)
		default:
			if colname, ok := whenExpr.(*sqlparser.ColName); ok {
				cols, tabs = append(cols, colname.Name.String()), append(tabs, colname.Qualifier.Name.String())
			} else {
				cols, tabs = extractColumns(whenExpr)
			}
		}

		columns = append(columns, cols...)
		tables = append(tables, tabs...)

		if _, ok := when.Val.(*sqlparser.ParenExpr); ok {
			when.Cond = when.Val.(*sqlparser.ParenExpr).Expr
		}

		switch whenValExpr := when.Val.(type) {
		case *sqlparser.CaseExpr:
			cols, tabs = caseHandler(whenValExpr)
		case *sqlparser.FuncExpr:
			cols, tabs = funcHandler(whenValExpr)
		case *sqlparser.BinaryExpr:
			cols, tabs = binaryHandler(whenValExpr)
		case *sqlparser.AndExpr:
			cols, tabs = extractColumns(whenValExpr)
		case *sqlparser.OrExpr:
			cols, tabs = extractColumns(whenValExpr)
		case *sqlparser.ComparisonExpr:
			cols, tabs = extractColumns(whenValExpr)
		default:
			if colname, ok := whenValExpr.(*sqlparser.ColName); ok {
				cols, tabs = append(cols, colname.Name.String()), append(tabs, colname.Qualifier.Name.String())
			} else {
				cols, tabs = extractColumns(whenValExpr)
			}
		}

		columns = append(columns, cols...)
		tables = append(tables, tabs...)

		// for _, inExpr := range sqlparser.Iterate(when.Cond) {
		// 	switch inExpr := inExpr.(type) {
		// 	case *sqlparser.ColName:
		// 		fmt.Println("Table :", inExpr.Qualifier.String())
		// 	}
		// }
	}

	var cols []string
	_ = cols
	var tabs []string
	_ = tabs

	// fmt.Println("elsepart")

	if _, ok := casePart.Else.(*sqlparser.ParenExpr); ok {
		casePart.Else = casePart.Else.(*sqlparser.ParenExpr).Expr
	}

	switch elsePart := casePart.Else.(type) {
	case *sqlparser.CaseExpr:
		cols, tabs = caseHandler(elsePart)
	case *sqlparser.FuncExpr:
		cols, tabs = funcHandler(elsePart)
	case *sqlparser.BinaryExpr:
		cols, tabs = binaryHandler(elsePart)
	case *sqlparser.AndExpr:
		cols, tabs = extractColumns(elsePart)
	case *sqlparser.OrExpr:
		cols, tabs = extractColumns(elsePart)
	case *sqlparser.ComparisonExpr:
		cols, tabs = extractColumns(elsePart)
	default:
		// fmt.Printf("reached here %t\n", elsePart)
		if colname, ok := elsePart.(*sqlparser.ColName); ok {
			cols, tabs = append(cols, colname.Name.String()), append(tabs, colname.Qualifier.Name.String())
		} else {
			cols, tabs = extractColumns(elsePart)
		}
	}

	columns = append(columns, cols...)
	tables = append(tables, tabs...)
	if colname, ok := casePart.Else.(*sqlparser.ColName); ok {
		tables = append(tables, colname.Qualifier.Name.String())
		columns = append(columns, colname.Name.String())
	}
	// fmt.Printf("\tElse expression: %v\n", sqlparser.String(casePart.Else))

	// fmt.Println("Columns of Case expression : ", columns)
	// fmt.Println("Tables of Case expression : ", tables)

	return columns, tables
}

func funcHandler(funcPart *sqlparser.FuncExpr) ([]string, []string) {
	var columns []string
	var tables []string
	_ = tables
	_ = columns

	// fmt.Println("function detected", funcPart.Name.String())

	for _, funcExp := range funcPart.Exprs {

		var cols []string
		_ = cols
		var tabs []string
		_ = tabs

		switch expr := funcExp.(type) {
		case *sqlparser.AliasedExpr:
			switch expr.Expr.(type) {
			case *sqlparser.FuncExpr:
				funcTemp := expr.Expr.(*sqlparser.FuncExpr)
				cols, tabs = funcHandler(funcTemp)
			case *sqlparser.CaseExpr:
				caseTemp := expr.Expr.(*sqlparser.CaseExpr)
				cols, tabs = caseHandler(caseTemp)
			case *sqlparser.BinaryExpr:
				binaryTemp := expr.Expr.(*sqlparser.BinaryExpr)
				cols, tabs = binaryHandler(binaryTemp)
			case *sqlparser.AndExpr:
				cols, tabs = extractColumns(expr.Expr)
			case *sqlparser.OrExpr:
				cols, tabs = extractColumns(expr.Expr)
			case *sqlparser.ComparisonExpr:
				cols, tabs = extractColumns(expr.Expr)
			default:
				// cols, tabs = extractColumns(expr.Expr)
				if colname, ok := expr.Expr.(*sqlparser.ColName); ok {
					cols, tabs = append(cols, colname.Name.String()), append(tabs, colname.Qualifier.Name.String())
					// fmt.Printf("Table: %s Column: %s Alias: %s\n\n", colname.Qualifier.Name.String(), colname.Name.String(), expr.As)
				} else {
					cols, tabs = extractColumns(expr.Expr)
				}
				// } else {
				// 	fmt.Printf("Column: %s Alias: %s\n\n", sqlparser.String(expr.Expr), expr.As)
				// }
			}
		}
		// switch funcExp := funcExp.(type) {
		// case *sqlparser.FuncExpr:
		// 	cols, tabs = funcHandler(funcExp)
		// case *sqlparser.CaseExpr:
		// 	cols, tabs = caseHandler(funcExp)
		// case *sqlparser.BinaryExpr:
		// 	binaryHandler(funcExp)
		// case *sqlparser.AndExpr:
		// 	cols, tabs = extractColumns(funcExp)
		// case *sqlparser.OrExpr:
		// 	cols, tabs = extractColumns(funcExp)
		// case *sqlparser.ComparisonExpr:
		// 	cols, tabs = extractColumns(funcExp)
		// default:
		// 	cols, tabs = extractColumns(funcExp)
		// }

		columns = append(columns, cols...)
		tables = append(tables, tabs...)
	}
	// fmt.Println(columns, tables)
	return columns, tables
}

func binaryHandler(binaryPart *sqlparser.BinaryExpr) ([]string, []string) {
	var columns []string
	var tables []string
	_ = tables
	_ = columns

	var cols []string
	var tabs []string
	_ = tabs
	_ = cols

	// fmt.Println("Binary expression detected")
	// fmt.Println(string(binaryPart.Operator))

	switch leftPart := binaryPart.Left.(type) {
	case *sqlparser.CaseExpr:
		cols, tabs = caseHandler(leftPart)
	case *sqlparser.FuncExpr:
		cols, tabs = funcHandler(leftPart)
	case *sqlparser.BinaryExpr:
		cols, tabs = binaryHandler(leftPart)
	case *sqlparser.AndExpr:
		cols, tabs = extractColumns(leftPart)
	case *sqlparser.OrExpr:
		cols, tabs = extractColumns(leftPart)
	case *sqlparser.ComparisonExpr:
		cols, tabs = extractColumns(leftPart)
	default:
		if colname, ok := leftPart.(*sqlparser.ColName); ok {
			cols, tabs = append(cols, colname.Name.String()), append(tabs, colname.Qualifier.Name.String())
		} else {
			cols, tabs = extractColumns(leftPart)
		}
	}

	columns = append(columns, cols...)
	tables = append(tables, tabs...)

	if _, ok := binaryPart.Right.(*sqlparser.ParenExpr); ok {
		binaryPart.Right = binaryPart.Right.(*sqlparser.ParenExpr).Expr
	}

	switch rightPart := binaryPart.Right.(type) {
	case *sqlparser.CaseExpr:
		cols, tabs = caseHandler(rightPart)
	case *sqlparser.FuncExpr:
		cols, tabs = funcHandler(rightPart)
	case *sqlparser.BinaryExpr:
		cols, tabs = binaryHandler(rightPart)
	case *sqlparser.AndExpr:
		cols, tabs = extractColumns(rightPart)
	case *sqlparser.OrExpr:
		cols, tabs = extractColumns(rightPart)
	case *sqlparser.ComparisonExpr:
		cols, tabs = extractColumns(rightPart)
	default:
		if colname, ok := rightPart.(*sqlparser.ColName); ok {
			cols, tabs = append(cols, colname.Name.String()), append(tabs, colname.Qualifier.Name.String())
		} else {
			cols, tabs = extractColumns(rightPart)
		}
	}

	columns = append(columns, cols...)
	tables = append(tables, tabs...)
	// fmt.Println(columns, tables)
	return columns, tables
}

func mainParserFunction(expr *sqlparser.AliasedExpr) []queryInfo {
	var queryData []queryInfo
	var columns []string
	var tables []string
	_ = tables
	_ = columns

	switch expr.Expr.(type) {
	case *sqlparser.FuncExpr:
		funcPart := expr.Expr.(*sqlparser.FuncExpr)
		columns, tables = funcHandler(funcPart)
		columns, tables = removeDuplicates(columns, tables)
		// if len(columns) == 0 {
		// 	if colname, ok := expr.Expr.(*sqlparser.ColName); ok {
		// 		fmt.Printf("Table: %s Column: %s Alias: %s\n\n", colname.Qualifier.Name.String(), colname.Name.String(), expr.As)
		// 	} else {
		// 		fmt.Printf("Column: %s Alias: %s\n\n", sqlparser.String(expr.Expr), expr.As)
		// 	}
		// }
		// fmt.Println("Columns of Function expression : ", columns)
		// fmt.Println("Tables of Function expression : ", tables)
		// fmt.Printf("Alias: %s\n", expr.As)
		queryData = append(queryData, queryInfo{
			TableAliasNames: tables,
			Columns:         columns,
			Alias:           expr.As.String(),
			Expression:      sqlparser.String(funcPart),
		})
	case *sqlparser.CaseExpr:
		casePart := expr.Expr.(*sqlparser.CaseExpr)
		columns, tables = caseHandler(casePart)
		columns, tables = removeDuplicates(columns, tables)
		// fmt.Println("Columns of Case expression : ", columns)
		// fmt.Println("Tables of Case expression : ", tables)
		// fmt.Printf("Alias: %s\n", expr.As)
		queryData = append(queryData, queryInfo{
			TableAliasNames: tables,
			Columns:         columns,
			Alias:           expr.As.String(),
			Expression:      sqlparser.String(casePart),
		})
	case *sqlparser.BinaryExpr:
		binaryPart := expr.Expr.(*sqlparser.BinaryExpr)
		columns, tables = binaryHandler(binaryPart)
		columns, tables = removeDuplicates(columns, tables)
		queryData = append(queryData, queryInfo{
			TableAliasNames: tables,
			Columns:         columns,
			Alias:           expr.As.String(),
			Expression:      sqlparser.String(binaryPart),
		})
	default:
		var alias string
		_ = alias
		if sqlparser.String(expr.As) != "" {
			if colname, ok := expr.Expr.(*sqlparser.ColName); ok {
				// fmt.Printf("Table: %s Column: %s Alias: %s\n\n", colname.Qualifier.Name.String(), colname.Name.String(), expr.As)
				tables = append(tables, colname.Qualifier.Name.String())
				columns = append(columns, colname.Name.String())
				alias = expr.As.String()
			} else {
				// fmt.Printf("Column: %s Alias: %s\n\n", sqlparser.String(expr.Expr), expr.As)
				columns = append(columns, sqlparser.String(expr.Expr))
				alias = expr.As.String()
			}
		} else {
			if colname, ok := expr.Expr.(*sqlparser.ColName); ok {
				// fmt.Printf("Table: %s Column: %s Alias: %s\n\n", colname.Qualifier.Name.String(), colname.Name.String(), colname.Name.String())
				tables = append(tables, colname.Qualifier.Name.String())
				columns = append(columns, colname.Name.String())
				alias = colname.Name.String()
			} else {
				// fmt.Printf("Column: %s Alias: %s\n\n", sqlparser.String(expr.Expr), expr.As)
				columns = append(columns, sqlparser.String(expr.Expr))
				alias = expr.As.String()
			}
		}
		columns, tables = removeDuplicates(columns, tables)
		queryData = append(queryData, queryInfo{
			TableAliasNames: tables,
			Columns:         columns,
			Alias:           alias,
			Expression:      sqlparser.String(expr.Expr),
		})
	}
	return queryData
}

func Optimizer(call string) {
	fmt.Println(call)
	var filename string
	var queryData []queryInfo
	var joinData []joinExpression

	var input []int
	var aliasInputs []string

	var finalQuerySelectExpressionsList []string
	var finalQueryJoinExpressionsList []string

	var finalQuerySelectExpression string
	_ = finalQuerySelectExpression
	var finalQueryJoinExpression string
	_ = finalQueryJoinExpression

	var leftTable string
	_ = leftTable
	var leftTableAlias string
	_ = leftTableAlias

	var optimizedQuery string
	_ = optimizedQuery

	var aliasNames = []string{
		"order_id",
		"alternative_legacy_order_id",
		"order_status",
		"account_id",
		"account_name",
		"certificate_id",
		"certificate_type",
		"product_name",
		"product_type",
		"product_name_id",
		"container_name",
		"container_id",
		"container_status",
		"order_created_date",
		"certificate_requested_date",
		"order_validity_years",
		"order_expiration_date",
		"order_email_client_certificate",
		"additional_emails",
		"order_placed_via",
		"order_month",
		"order_year",
		"server_license",
		"server_type",
		"number_of_sans",
		"contains_wildcard",
		"purchased_wildcard_sans",
		"purchased_non_wildcard_fqdns",
		"auto_renew",
		"is_renewed",
		"renewed_order_id",
		"custom_renewal_message",
		"disable_renewal_notifications",
		"reissued_order_new_sans",
		"reissued_order_old_sans",
		"reissued_order_new_cn",
		"reissued_order_old_cn",
		"certificate_reissue_date",
		"organization_contact_name",
		"organization_contact_email",
		"organization_contact_job_title",
		"organization_contact_telephone",
		"technical_contact_name",
		"technical_contact_email",
		"technical_contact_job_title",
		"technical_contact_telephone",
		"user_requestor_name",
		"user_requestor_email",
		"user_requestor_id",
		"user_approver_name",
		"user_approver_email",
		"user_approver_id",
		"billing_contact_name",
		"billing_contact_email",
		"billing_contact_organization_name",
		"billing_address_line_1",
		"billing_address_line_2",
		"billing_address_city",
		"billing_address_state",
		"billing_address_country",
		"billing_address_zip_code",
		"shipping_name",
		"shipping_address_line_1",
		"shipping_address_line_2",
		"shipping_city",
		"shipping_state",
		"shipping_country",
		"shipping_zip_code",
		"account_currency",
		"purchase_amount",
		"estimated_tax",
		"transaction_date",
		"transaction_type",
		"payment_method",
		"provisioning_method",
		"receipt_id",
		"invoice_id",
		"net_price",
		"total_units",
		"deal_id",
		"unit_id",
		"multi_year_plan",
		"competitive_replacement_benefit_additional_days",
		"competitive_replacement_benefit_percentage",
		"competitive_replacement_order_actual_price",
		"subaccount_name",
		"parent_account_pricing",
		"parent_account_currency",
		"subaccount_pricing",
		"subaccount_currency",
		"subaccount_container_id",
		"common_name",
		"sans",
		"dcv_method",
		"certificate_status",
		"validity_start_date",
		"validity_end_date",
		"certificate_validity_in_days",
		"days_remaining_until_expiration",
		"csr",
		"pem",
		"root",
		"intermediate_ca",
		"intermediate_ca_id",
		"serial_number",
		"signature_hash",
		"thumbprint",
		"organization_id",
		"organization_name",
		"organization_unit",
		"country",
		"state",
		"locality",
		"logged_to_public_ct",
	}

	var displayNames = []string{
		"Order details:1:Order information:1 -> Order ID:INTEGER:REQUIRED",
		"Order details:1:Order information:1 -> Alternative/Legacy order ID",
		"Order details:1:Order information:1 -> Order status:['Issued','Pending','Reissue pending','Renewed','Revoked','Rejected','Expired','Waiting pickup', 'Canceled']#DEFAULT",
		"Order details:1:Order information:1 -> Account ID#DEFAULT",
		"Order details:1:Order information:1 -> Account name",
		"Order details:1:Order information:1 -> Certificate ID:INTEGER",
		"Order details:1:Order information:1 -> Request state#DEFAULT",
		"Order details:1:Order information:1 -> Product name#DEFAULT",
		"Order details:1:Order information:1 -> Product type",
		"Order details:1:Order information:1 -> Product ID",
		"Order details:1:Order information:1 -> Division/Container name",
		"Order details:1:Order information:1 -> Division/Container ID",
		"Order details:1:Order information:1 -> Division/Container status",
		"Order details:1:Order information:1 -> Order created date:DATE:REQUIRED",
		"Order details:1:Order information:1 -> Certificate requested date",
		"Order details:1:Order information:1 -> Order validity years",
		"Order details:1:Order information:1 -> Order expiration date",
		"Order details:1:Order information:1 -> Order email (client certificate)",
		"Order details:1:Order information:1 -> Additional email#DEFAULT",
		"Order details:1:Order information:1 -> Order placed via:['API','CertCentral', 'Guest Access', 'Guest URL']",
		"Order details:1:Order information:1 -> Order month:INTEGER",
		"Order details:1:Order information:1 -> Order year:INTEGER",
		"Order details:1:Order information:1 -> Server license",
		"Order details:1:Order information:1 -> Server type",
		"Order details:1:Order information:1 -> Number of SANs",
		"Order details:1:Order information:1 -> Contains wildcard",
		"Order details:1:Order information:1 -> Purchased wildcard SANs",
		"Order details:1:Order information:1 -> Purchased non wildcard FQDN",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Auto renew",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Is renewed",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Renewed order ID",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Custom renewal message",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Disabled renewal notifications",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Reissued order new SANs",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Reissued order old SANs",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Reissued order new CN",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Reissued order old CN",
		"Order details:1:Renewal, reissue, and duplicate information:2 -> Certificate reissue/duplicate date",
		"Order details:1:Contact information:3 -> Organization contact name",
		"Order details:1:Contact information:3 -> Organization contact email",
		"Order details:1:Contact information:3 -> Organization contact job title",
		"Order details:1:Contact information:3 -> Organization contact telephone",
		"Order details:1:Contact information:3 -> Technical contact name",
		"Order details:1:Contact information:3 -> Technical contact email",
		"Order details:1:Contact information:3 -> Technical contact job title",
		"Order details:1:Contact information:3 -> Technical contact telephone",
		"Order details:1:Contact information:3 -> User/Requester name#DEFAULT",
		"Order details:1:Contact information:3 -> User/Requester email#DEFAULT",
		"Order details:1:Contact information:3 -> User/Requester ID",
		"Order details:1:Contact information:3 -> User/Approver name#DEFAULT",
		"Order details:1:Contact information:3 -> User/Approver email#DEFAULT",
		"Order details:1:Contact information:3 -> User/Approver ID",
		"Order details:1:Billing and shipping information:4 -> Billing contact name",
		"Order details:1:Billing and shipping information:4 -> Billing contact email",
		"Order details:1:Billing and shipping information:4 -> Billing contact organization name",
		"Order details:1:Billing and shipping information:4 -> Billing address line 1",
		"Order details:1:Billing and shipping information:4 -> Billing address line 2",
		"Order details:1:Billing and shipping information:4 -> Billing address city",
		"Order details:1:Billing and shipping information:4 -> Billing address state",
		"Order details:1:Billing and shipping information:4 -> Billing address country",
		"Order details:1:Billing and shipping information:4 -> Billing address zip code",
		"Order details:1:Billing and shipping information:4 -> Shipping name",
		"Order details:1:Billing and shipping information:4 -> Shipping address line 1",
		"Order details:1:Billing and shipping information:4 -> Shipping address line 2",
		"Order details:1:Billing and shipping information:4 -> Shipping city",
		"Order details:1:Billing and shipping information:4 -> Shipping state",
		"Order details:1:Billing and shipping information:4 -> Shipping country",
		"Order details:1:Billing and shipping information:4 -> Shipping zip code",
		"Order details:1:Payment and transaction information:5 -> Account currency",
		"Order details:1:Payment and transaction information:5 -> Purchase amount",
		"Order details:1:Payment and transaction information:5 -> Estimated tax",
		"Order details:1:Payment and transaction information:5 -> Transaction date",
		"Order details:1:Payment and transaction information:5 -> Transaction type",
		"Order details:1:Payment and transaction information:5 -> Payment method:['Account balance','Credit Card','Voucher','Wire Transfer','Unit','PO']",
		"Order details:1:Payment and transaction information:5 -> Provisioning method",
		"Order details:1:Payment and transaction information:5 -> Receipt ID",
		"Order details:1:Payment and transaction information:5 -> Wire transfer order invoice ID",
		"Order details:1:Payment and transaction information:5 -> Net price",
		"Order details:1:Payment and transaction information:5 -> Total units",
		"Order details:1:Payment and transaction information:5 -> Deal ID",
		"Order details:1:Payment and transaction information:5 -> Unit ID",
		"Order details:1:Payment and transaction information:5 -> Multi year plan",
		"Order details:1:Payment and transaction information:5 -> Competitive replacement benefit additional days",
		"Order details:1:Payment and transaction information:5 -> Competitive replacement benefit percentage",
		"Order details:1:Payment and transaction information:5 -> Competitive replacement order actual price",
		"Order details:1:Subaccount information:6 -> Subaccount -> Subaccount name",
		"Order details:1:Subaccount information:6 -> Subaccount -> Parent account pricing#DEFAULT",
		"Order details:1:Subaccount information:6 -> Subaccount -> Parent account currency",
		"Order details:1:Subaccount information:6 -> Subaccount -> Subaccount pricing",
		"Order details:1:Subaccount information:6 -> Subaccount -> Subaccount currency",
		"Order details:1:Subaccount information:6 -> Subaccount -> Subaccount division/container ID",
		"Certificate details:2:Certificate information:1 -> Common name#DEFAULT",
		"Certificate details:2:Certificate information:1 -> SANs#DEFAULT",
		"Certificate details:2:Certificate information:1 -> DCV method",
		"Certificate details:2:Certificate information:1 -> Certificate status:['Issued','Pending','Reissue pending','Renewed','Revoked','Rejected','Expired','Waiting pickup']#DEFAULT",
		"Certificate details:2:Certificate information:1 -> Validity start date#DEFAULT",
		"Certificate details:2:Certificate information:1 -> Validity end date#DEFAULT",
		"Certificate details:2:Certificate information:1 -> Certificate validity in days",
		"Certificate details:2:Certificate information:1 -> Days remaining until expiration:INTEGER",
		"Certificate details:2:Certificate information:1 -> CSR",
		"Certificate details:2:Certificate information:1 -> Certificate (PEM format)",
		"Certificate details:2:Certificate information:1 -> Root",
		"Certificate details:2:Certificate information:1 -> Intermediate CA",
		"Certificate details:2:Certificate information:1 -> Intermediate CA ID",
		"Certificate details:2:Certificate information:1 -> Serial number#DEFAULT",
		"Certificate details:2:Certificate information:1 -> Signature hash",
		"Certificate details:2:Certificate information:1 -> Thumbprint",
		"Certificate details:2:Certificate information:1 -> Organization ID",
		"Certificate details:2:Certificate information:1 -> Organization name#DEFAULT",
		"Certificate details:2:Certificate information:1 -> Organization unit",
		"Certificate details:2:Certificate information:1 -> Country",
		"Certificate details:2:Certificate information:1 -> State",
		"Certificate details:2:Certificate information:1 -> Locality",
		"Certificate details:2:Certificate information:1 -> Logged to public Certificate Transparency (CT) logs",
	}

	fmt.Println()
	for i, name := range displayNames {
		fmt.Printf("%s. %s\n", strconv.Itoa(i), name)
	}

	fmt.Println("\n(Type 'all' to include all columns)")
	fmt.Println("(Type 'default' to include only DEFAULT columns)")
	fmt.Println("(Type 'done' to stop after entering custom column indexes)")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		inputIndex := scanner.Text()

		if inputIndex == "all" {
			input = input[:0]
			for i := 0; i < len(aliasNames); i++ {
				input = append(input, i)
			}
			// fmt.Println(len(input))
			break
		}

		if inputIndex == "default" {
			input = input[:0]
			input = []int{2, 3, 6, 7, 18, 46, 47, 49, 50, 86, 91, 92, 94, 95, 96, 104, 108}
			break
		}

		if inputIndex == "done" {
			break
		}

		index, err := strconv.Atoi(inputIndex)
		if err != nil {
			fmt.Println("Something went wrong! Check your input")
			continue
		}
		input = append(input, index)
	}

	sort.Ints(input)

	for _, ind := range input {
		aliasInputs = append(aliasInputs, aliasNames[ind])
	}

	fmt.Println(aliasInputs)

	fmt.Println("\nEnter the filename:")
	_, err := fmt.Scanln(&filename)
	checkError(err)

	data, err := ioutil.ReadFile(filename)
	checkError(err)

	tempData := string(data)

	queries, err := sqlparser.SplitStatementToPieces(preprocessing(tempData))
	fmt.Println(len(queries))
	checkError(err)

	query, err := sqlparser.Parse(queries[0])
	checkError(err)

	// sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
	// 	switch node := node.(type) {
	// 	case *sqlparser.ParenExpr:
	// 		if _, ok := node.Expr.(*sqlparser.CaseExpr); ok {
	// 			node.Expr = node.Expr.(*sqlparser.CaseExpr).Expr
	// 		}
	// 	}
	// 	return true, nil
	// }, query)

	// print(sqlparser.String(query))

	// fmt.Printf("Type %T\n", query)

	selectStatement := query.(*sqlparser.Select)

	fmt.Println("Column names and their aliases are :")
	fmt.Println()

	for _, selExpr := range selectStatement.SelectExprs {

		switch expr := selExpr.(type) {
		case *sqlparser.AliasedExpr:
			if slices.Contains(aliasInputs, expr.As.String()) {
				queryData = append(queryData, mainParserFunction(expr)...)
			} else if colname, ok := expr.Expr.(*sqlparser.ColName); ok {
				if slices.Contains(aliasInputs, colname.Name.String()) {
					queryData = append(queryData, mainParserFunction(expr)...)
				}
			}
			// else {
			// 	fmt.Printf("didn't match with %s\n", expr.As)
			// }
			// if casePart, ok := expr.Expr.(*sqlparser.CaseExpr); ok {

			// } else {
			// 	// fmt.Printf("Table: %s Column: %s Alias: %s\n", expr.Expr.(*sqlparser.ColName).Name.String(), sqlparser.String(expr.Expr), expr.As)
			// 	// fmt.Println()
			// 	if colname, ok := expr.Expr.(*sqlparser.ColName); ok {
			// 		fmt.Printf("Table: %s Column: %s Alias: %s\n\n", colname.Qualifier.Name.String(), colname.Name.String(), expr.As)
			// 	} else {
			// 		fmt.Printf("Column: %s Alias: %s\n\n", sqlparser.String(expr.Expr), expr.As)
			// 	}
			// }
		default:
			// fmt.Printf("Column: %s\n", sqlparser.String(selExpr))
		}
	}

	fromClause := selectStatement.From
	// var tablenames []string
	// tableMap := make(map[string]string)

	// filling the joinData slice of structs. The parsing happens from the last join expression. Therefore, we recursively parse the leftpart and append
	// the right part to the joinData
	for _, tableExpr := range fromClause {
		switch t := tableExpr.(type) {
		case *sqlparser.JoinTableExpr:
			j := t
			for {
				// switch leftPart := j.LeftExpr.(type) {
				// case *sqlparser.JoinTableExpr:
				// 	_ = leftPart
				// 	fmt.Println("Reached here for left")
				// }
				// fmt.Println(sqlparser.String(j.Condition.On))
				joinType := j.Join
				onCond := sqlparser.String(j.Condition.On)
				columns, tables := extractColumns(j.Condition.On)
				columns, tables = removeDuplicates(columns, tables)
				switch right := j.RightExpr.(type) {
				case *sqlparser.AliasedTableExpr:
					// tablenames = append(tablenames, sqlparser.String(right.Expr))
					// tableMap[sqlparser.String(right.As)] = sqlparser.String(right.Expr)
					rightTableTemp := ""
					_ = rightTableTemp

					// handling the condition where right table is a subquery
					if selExpr, ok := right.Expr.(*sqlparser.Subquery); ok {
						nestedSelect, _ := selExpr.Select.(*sqlparser.Select)
						for _, tableExpr := range nestedSelect.From {
							if at, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
								rightTableTemp = sqlparser.String(at.Expr)
							}
						}
					} else {
						rightTableTemp = sqlparser.String(right.Expr)
					}
					joinData = append(joinData, joinExpression{
						JoinType:            joinType,
						RightTableAliasName: sqlparser.String(right.As),
						RightTable:          rightTableTemp,
						OnCondition:         onCond,
						Tables:              tables,
						Columns:             columns,
						JoinDependencyList:  []string{strings.ToUpper(joinType) + " " + rightTableTemp + " " + sqlparser.String(right.As) + " ON " + onCond},
					})
				}

				if jLeft, ok := j.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
					// tablenames = append(tablenames, sqlparser.String(jLeft.Expr))
					// tableMap[sqlparser.String(jLeft.As)] = sqlparser.String(jLeft.Expr)
					for i := range joinData {
						joinData[i].LeftTableAliasName = sqlparser.String(jLeft.As)
						joinData[i].LeftTable = sqlparser.String(jLeft.Expr)
					}
					leftTable = sqlparser.String(jLeft.Expr)
					leftTableAlias = sqlparser.String(jLeft.Expr)
					break
				}

				j = j.LeftExpr.(*sqlparser.JoinTableExpr)
			}
		}
	}

	// reversing the joinData because the parsing has happened from last join to the first join
	reverseSliceOfStruct(joinData)

	// creating the dependency tree. If the Tables have entries other than customer_order_1 and the aliasname itself, then we fill the dependencies recursively.
	for i := range joinData {
		// if len(joinData[i].Tables) > 2 {
		for _, tableName := range joinData[i].Tables {
			if tableName != joinData[i].LeftTableAliasName && tableName != joinData[i].RightTableAliasName {
				index := slices.IndexFunc(joinData, func(j joinExpression) bool {
					return j.RightTableAliasName == tableName
				})

				if index != -1 {
					joinData[i].Dependencies = append(joinData[i].Dependencies, joinData[index])
					// dependency := joinData[index].RightTableAliasName + " " + joinData[index].RightTable + " ON " + joinData[index].OnCondition
					joinData[i].JoinDependencyList = append(joinData[i].JoinDependencyList, joinData[index].JoinDependencyList...)
				} else {
					tableIndex := slices.IndexFunc(joinData, func(j joinExpression) bool {
						return j.RightTable == tableName
					})
					if tableIndex != -1 {
						joinData[i].Dependencies = append(joinData[i].Dependencies, joinData[tableIndex])
						// dependency := joinData[tableIndex].RightTableAliasName + " " + joinData[tableIndex].RightTable + " ON " + joinData[tableIndex].OnCondition
						joinData[i].JoinDependencyList = append(joinData[i].JoinDependencyList, joinData[tableIndex].JoinDependencyList...)
					} else {
						fmt.Printf("Not found above! %s\n", tableName)
					}
				}
			}
		}
		// }
	}

	for joinIndex := range joinData {
		for queryIndex := range queryData {
			if slices.Contains(queryData[queryIndex].TableAliasNames, joinData[joinIndex].RightTableAliasName) {
				queryData[queryIndex].Tables = append(queryData[queryIndex].Tables, joinData[joinIndex].RightTable)
				queryData[queryIndex].JoinExpression = append(queryData[queryIndex].JoinExpression, joinData[joinIndex])
			}
			if slices.Contains(queryData[queryIndex].TableAliasNames, joinData[joinIndex].LeftTableAliasName) && !(slices.Contains(queryData[queryIndex].Tables, joinData[joinIndex].LeftTable)) {
				queryData[queryIndex].Tables = append(queryData[queryIndex].Tables, joinData[joinIndex].LeftTable)
			}
		}
	}

	queryJSON, err := json.MarshalIndent(queryData, "", "\t")
	checkError(err)

	for i := range queryData {
		aliasExpr := queryData[i].Expression + " AS " + queryData[i].Alias + ", "
		finalQuerySelectExpressionsList = append(finalQuerySelectExpressionsList, aliasExpr)
		if queryData[i].JoinExpression != nil {
			for _, join := range queryData[i].JoinExpression {
				reverseSliceofStrings(join.JoinDependencyList)
				finalQueryJoinExpressionsList = append(finalQueryJoinExpressionsList, join.JoinDependencyList...)
			}
		}
	}

	finalQuerySelectExpressionsList = cleanList(finalQuerySelectExpressionsList)
	finalQueryJoinExpressionsList = cleanList(finalQueryJoinExpressionsList)

	finalQuerySelectExpressionsList = append(finalQuerySelectExpressionsList, "@revocation_date_column,")

	finalQuerySelectExpression = strings.Join(finalQuerySelectExpressionsList, "\n")
	finalQueryJoinExpression = strings.Join(finalQueryJoinExpressionsList, "\n")

	optimizedQuery = "SELECT\n" + finalQuerySelectExpression + "\nFROM " + leftTable + " " + leftTableAlias + "\n" + finalQueryJoinExpression

	optimizedQuery = finalProcessing(optimizedQuery)

	// fmt.Println(finalQuerySelectExpression)
	// fmt.Println()
	// fmt.Println(finalQueryJoinExpression)

	// joinJSON, err := json.MarshalIndent(joinData, "", "\t")
	// checkError(err)

	file, err := os.Create("parsed_query3.json")
	checkError(err)

	defer file.Close()

	_, err = file.Write(queryJSON)
	checkError(err)

	file, err = os.Create("optimized_query3.sql")
	checkError(err)

	defer file.Close()

	_, err = file.Write([]byte(optimizedQuery))
	checkError(err)

	// file, err = os.Create("parsed_joins.json")
	// checkError(err)

	// defer file.Close()

	// _, err = file.Write(joinJSON)
	// checkError(err)

	fmt.Println("JSON data written to parsed_query.json")
}

// joinClauses := selectStatement.From[0].(*sqlparser.JoinTableExpr)

// for {
// 	table := sqlparser.String(joinClauses.RightExpr)
// 	fmt.Println("Table name:", table)

// 	joinCondition := sqlparser.String(joinClauses.Condition)
// 	fmt.Println("Condition:", joinCondition)

// 	if joinClauses.RightExpr == nil {
// 		break
// 	}

// 	joinClauses = joinClauses.RightExpr.(*sqlparser.JoinTableExpr)
// 	fmt.Printf("Table: %s, Condition: %s", table, joinCondition)
// }

// for _, tableExpr := range fromClause {
// 	switch t := tableExpr.(type) {
// 	case *sqlparser.JoinTableExpr:
// 		j := t
// 		// fmt.Println(sqlparser.String(j.LeftExpr))
// 		// fmt.Printf("%t", j.LeftExpr)
// 		switch left := j.LeftExpr.(type) {
// 		case *sqlparser.JoinTableExpr:
// 			_ = left
// 			fmt.Println("Reached here for left")
// 		}
// 		fmt.Println(sqlparser.String(j.Condition.On))
// 		fmt.Println(sqlparser.String(j.RightExpr))
// 		switch right := j.RightExpr.(type) {
// 		case *sqlparser.AliasedTableExpr:
// 			tablenames = append(tablenames, sqlparser.String(right.As))
// 		}
// 	}
// }
