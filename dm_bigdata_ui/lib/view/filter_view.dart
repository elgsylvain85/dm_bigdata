import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:dm_bigdata_ui/view/home_view.dart';
import 'package:flutter/material.dart';
import 'package:multi_select_flutter/multi_select_flutter.dart';

class FilterView extends StatefulWidget {
  final _structureService = WebAPIService();
  final _utilities = Utilities();
  // final _dropdownFilesNamesKey = GlobalKey<FormFieldState>();
  final _multiSelectFilesNamesKey1 = GlobalKey<FormFieldState>();
  final _dropdownColumnsFilter = <GlobalKey<FormFieldState>>[];
  final _textFieldColumnsFilter = <GlobalKey<FormFieldState>>[];

  var dataInitialized = false;
  var dataLoading = false;

  var tablesNames = <String>[];
  var columnsNames = <String>[];
  var filtersCount = 0;

  FilterView({Key? key}) : super(key: key);

  @override
  State<StatefulWidget> createState() => _FilterViewState();
}

class _FilterViewState extends State<FilterView> {
  @override
  void didChangeDependencies() {
    super.didChangeDependencies();

    if (!widget.dataInitialized) {
      refreshData();
      widget.dataInitialized = true;
    }
  }

  @override
  Widget build(BuildContext context) {
    dynamic item;

    /* Files names items */

    var filesNamesImportedItems = <DropdownMenuItem<String?>>[];
    var filesNamesImportedItems1 = <MultiSelectItem<String>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("ALL"),
    );

    filesNamesImportedItems.add(item);

    for (var e in widget.tablesNames) {
      var fileName = e.split('/').last; // short file name

      var item = DropdownMenuItem<String?>(
        value: e,
        child: Text(fileName),
      );

      var item1 = MultiSelectItem<String>(e, fileName);

      filesNamesImportedItems.add(item);
      filesNamesImportedItems1.add(item1);
    }

    /* Build */

    return Center(
        child: SingleChildScrollView(
            child: Column(
                mainAxisAlignment: MainAxisAlignment.spaceAround,
                crossAxisAlignment: CrossAxisAlignment.center,
                children: [
          const Text(
            "Filter",
            style: Utilities.titleStyle,
          ),
          /* Refresh button */
          Center(
            child: widget.dataLoading
                ? const CircularProgressIndicator()
                : IconButton(
                    onPressed: () {
                      refreshData();
                    },
                    icon: const Icon(Icons.refresh)),
          ),
          /* Filters Parameters */
          Container(
            width: Utilities.formWidth,
            padding: const EdgeInsets.symmetric(vertical: 10),
            child: Column(
              children: [
                // DropdownButtonFormField<String?>(
                //     key: widget._dropdownFilesNamesKey,
                //     style: Utilities.itemStyle,
                //     icon: const Icon(Icons.search),
                //     decoration: const InputDecoration(
                //         label: Text(
                //           "File Name",
                //           style: Utilities.itemStyle,
                //         ),
                //         filled: true,
                //         fillColor: Utilities.fieldFillColor,
                //         border: OutlineInputBorder(),
                //         contentPadding: EdgeInsets.symmetric(horizontal: 10)),
                //     items: filesNamesImportedItems,
                //     onChanged: (value) {}),
                MultiSelectDialogField(
                  key: widget._multiSelectFilesNamesKey1,
                  searchHint: "ALL",
                  buttonText: const Text("File Name"),
                  buttonIcon: const Icon(Icons.search),
                  decoration: BoxDecoration(
                    border: Border.all(),
                  ),
                  listType: MultiSelectListType.LIST,
                  items: filesNamesImportedItems1,
                  onConfirm: (value) {},
                ),
                columnFilterViews(),
                Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      TextButton(
                        onPressed: () {
                          addColumnFilterView();
                        },
                        child: const Text("Add", style: Utilities.itemStyle),
                      ),
                      TextButton(
                        onPressed: () {
                          reduceColumnFilterView();
                        },
                        child: const Text("Remove", style: Utilities.itemStyle),
                      ),
                    ]),
                ElevatedButton(
                    onPressed: () {
                      applyFilter();
                    },
                    child: const Text(
                      "Submit",
                      style: Utilities.itemStyle,
                    ))
              ],
            ),
          ),
        ])));
  }

  void loadColumnsNames() {
    // setState(() {
    //   widget.dataLoading = true;
    // });

    widget._structureService.appColumns().then((value) {
      widget.columnsNames.clear();

      // setState(() {
      widget.columnsNames.addAll(value);
      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.columnsNames.clear();
    }).whenComplete(() {
      setState(() {
        // widget.dataLoading = false;
      });
    });
  }

  void loadTablesNames() {
    setState(() {
      widget.dataLoading = true;
    });

    widget._structureService.tablesNames().then((value) {
      widget.tablesNames.clear();

      // setState(() {
      widget.tablesNames.addAll(value);
      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.tablesNames.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  /// generate dynamic column filter views relative to [filtersCount]
  Widget columnFilterViews() {
    var rows = <Widget>[];

    /* prepares items of columns names */

    var columnsItems = <DropdownMenuItem<String>>[];
    for (int i = 0; i < widget.columnsNames.length; i++) {
      var colName = widget.columnsNames[i];

      var item = DropdownMenuItem(
        value: colName,
        child: Text(colName),
      );

      columnsItems.add(item);
    }

    /* generate filter components */

    for (int i = 0; i < widget.filtersCount; i++) {
      rows.add(
          // Container(
          //   margin: const EdgeInsets.symmetric(vertical: 10),
          //   child:
          Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          // dropdown columns name
          SizedBox(
              width: Utilities.fieldWidth,
              // height: Utilities.fieldHeight,
              child: DropdownButtonFormField<String>(
                  key: widget._dropdownColumnsFilter[i],
                  style: Utilities.itemStyle,
                  decoration: const InputDecoration(
                      filled: true,
                      fillColor: Utilities.fieldFillColor,
                      border: OutlineInputBorder(),
                      contentPadding: EdgeInsets.symmetric(horizontal: 10)),
                  items: columnsItems,
                  onChanged: (value) {})),

          // text field filter value
          SizedBox(
              width: Utilities.fieldWidth,
              // height: Utilities.fieldHeight,
              child: TextFormField(
                key: widget._textFieldColumnsFilter[i],
                style: Utilities.itemStyle,
                decoration: const InputDecoration(
                  filled: true,
                  fillColor: Utilities.fieldFillColor,
                  border: OutlineInputBorder(),
                  contentPadding: EdgeInsets.symmetric(horizontal: 10),
                ),
              ))
        ],
      ));
    }

    return Column(
      children: rows,
    );
  }

  void refreshData() {
    loadTablesNames();
    loadColumnsNames();
  }

  void applyFilter() {
    /* Filter applying */

    String? sourcesExpr;
    String? columnsExpr;
    String? filterExpr;

    /* if filename is not null then pass that as first filter argument on special column "Sources"*/

    // String? fileName = widget._dropdownFilesNamesKey.currentState?.value;
    List<String> filesNames =
        widget._multiSelectFilesNamesKey1.currentState?.value;

    for (var fileName in filesNames) {
      if (fileName != null) {
        var expr = widget._utilities
            .filterExpression(Utilities.sourceColumn, fileName);

        if (sourcesExpr != null && sourcesExpr.trim().isNotEmpty) {
          //use AND SQL expression
          sourcesExpr = "$sourcesExpr or $expr";
        } else {
          sourcesExpr = expr;
        }
      }
    }

    // create filter expression from columns if exist

    for (int i = 0; i < widget.filtersCount; i++) {
      String colName = widget._dropdownColumnsFilter[i].currentState?.value;
      String filterValue =
          widget._textFieldColumnsFilter[i].currentState?.value;

      var expr = widget._utilities.filterExpression(colName, filterValue);

      if (columnsExpr != null && columnsExpr.trim().isNotEmpty) {
        //use AND SQL expression
        columnsExpr = "$columnsExpr and $expr";
      } else {
        columnsExpr = expr;
      }
    }

    if (sourcesExpr != null && columnsExpr != null) {
      filterExpr = "($sourcesExpr) and ($columnsExpr)";
    } else if (sourcesExpr != null) {
      filterExpr = sourcesExpr;
    } else if (columnsExpr != null) {
      filterExpr = columnsExpr;
    }

    Navigator.pushNamedAndRemoveUntil(
        context, HomeView.routeName, (Route<dynamic> route) => false,
        arguments: {
          // HomeView.fileNameArg: fileName,
          HomeView.filterExprArg: filterExpr
        });
  }

  void addColumnFilterView() {
    setState(() {
      /* increase columns filter according columns names count */
      if (widget.filtersCount < widget.columnsNames.length) {
        widget.filtersCount++;

        /* add too global key to link dynamically to filter component */

        widget._dropdownColumnsFilter.add(GlobalKey<FormFieldState>());
        widget._textFieldColumnsFilter.add(GlobalKey<FormFieldState>());
      }
    });
  }

  void reduceColumnFilterView() {
    setState(() {
      /* reduce columns filters count but never < 0 */

      if (widget.filtersCount > 0) {
        widget.filtersCount--;

        /* remove too global key */
        widget._dropdownColumnsFilter.removeLast();
        widget._textFieldColumnsFilter.removeLast();
      }
    });
  }
}
