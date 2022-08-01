// import 'package:data_table_2/data_table_2.dart';
import 'package:flutter/material.dart';
import 'package:pluto_grid/pluto_grid.dart';

class Utilities {
  static String webAPI = "http://localhost:9191";
  // static const itemStyle = TextStyle(fontSize: 11);
  static const itemBoldStyle =
      TextStyle(fontSize: 12, fontWeight: FontWeight.bold);
  static const titleStyle =
      TextStyle(fontSize: 26, fontWeight: FontWeight.bold);
  static const title2Style = TextStyle(fontSize: 18);

  static const fieldFillColor = Color.fromRGBO(243, 243, 243, 1);
  static const fieldHeight = 30.0;
  static const fieldWidth = 200.0;

  static const formWidth = 440.0;

  static const limitDataLoading = 5000;

  static const defaultCellEmptyValue = "...";

  static const sourceColumn = "SOURCES";

  /// generate data table view dynamically
  // DataTable2 generateDataTable(
  //     List<String> columnsNames, List<List<String>> data,
  //     {ScrollController? scrollController}) {
  //   var columns = <DataColumn2>[];
  //   var rows = <DataRow2>[];

  //   var colCount = columnsNames.length;

  //   /* generate columns */

  //   for (int i = 0; i < colCount; i++) {
  //     var cn = columnsNames[i];
  //     columns.add(DataColumn2(
  //         size: ColumnSize.L,
  //         // fixedWidth: Utilities.fieldWidth,
  //         label: SelectableText(
  //           cn,
  //           style: Utilities.itemBoldStyle,
  //         )));
  //   }

  //   /* generate rows according columns count*/

  //   for (var row in data) {
  //     var cells = <DataCell>[];

  //     for (int i = 0; i < colCount; i++) {
  //       /* complete cellule with data and empty with default value */

  //       var cellule = "";
  //       if (i < row.length) {
  //         cellule = row[i];
  //       } else {
  //         cellule = Utilities.defaultCellEmptyValue;
  //       }

  //       cells.add(DataCell(SelectableText(cellule,
  //           style: Utilities.itemStyle,
  //           scrollPhysics: const NeverScrollableScrollPhysics())));
  //     }

  //     var dataRow = DataRow2(cells: cells);
  //     rows.add(dataRow);
  //   }

  //   return DataTable2(
  //     minWidth: columns.length * Utilities.fieldWidth,
  //     headingRowColor:
  //         MaterialStateColor.resolveWith((states) => Colors.blueGrey),
  //     scrollController: scrollController,
  //     columns: columns,
  //     rows: rows,
  //   );
  // }

  String filterExpression(String column, String value) {
    // return "$column ilike '%25$value%25'"; // %25 echap =>  %
    // return "$column ilike '_${value}_'";
    return "$column rlike '(?i)$value'"; // (?i) => case-insensitive regex
  }

  String cleanFilterExpr(String? expr) {
    if (expr != null) {
      return expr
          .replaceAll(r"%25", "")
          .replaceAll(r"(?i)", "")
          .replaceAll(RegExp(r"\bilike\b"), "=")
          .replaceAll(RegExp(r"\brlike\b"), "=")
          .replaceAll(RegExp(r"\blike\b"), "=");
    } else {
      return "";
    }
  }

  /// generate data table column dynamically
  List<PlutoColumn> generateColumnPlutoTable(List<String> columnsNames) {
    var columns = <PlutoColumn>[];

    /* generate columns */

    for (var columnName in columnsNames) {
      columns.add(PlutoColumn(
          title: columnName,
          field: columnName,
          type: PlutoColumnType.text(),
          readOnly: true));
    }

    return columns;
  }

  /// generate data table rows dynamically
  List<PlutoRow> generateRowsPlutoTable(
      List<List<String>> data, List<String> columnsNames) {
    var rows = <PlutoRow>[];

    for (var row in data) {
      var rowData = <String, PlutoCell>{};

      for (int i = 0; i < columnsNames.length; i++) {
        var colName = columnsNames[i];
        var cellValue = Utilities.defaultCellEmptyValue;

        if (i < row.length) {
          cellValue = row[i];
        }

        rowData.putIfAbsent(colName, () => PlutoCell(value: cellValue));
      }

      rows.add(PlutoRow(cells: rowData));
    }

    return rows;
  }
}
