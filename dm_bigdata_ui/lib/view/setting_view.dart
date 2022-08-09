import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';
import 'package:multi_select_flutter/dialog/multi_select_dialog_field.dart';
import 'package:multi_select_flutter/util/multi_select_item.dart';
import 'package:multi_select_flutter/util/multi_select_list_type.dart';

class SettingView extends StatefulWidget {
  final _webAPIService = WebAPIService();
  // final _textFieldEditColumnKey = GlobalKey<FormFieldState>();
  // final _dropdownEditColumnKey = GlobalKey<FormFieldState>();
  // final _textFieldAddColumnKey = GlobalKey<FormFieldState>();
  // final _dropdownDeleteColumnKey = GlobalKey<FormFieldState>();
  // final _dropdownDeleteTableKey = GlobalKey<FormFieldState>();

  var dataInitialized = false;
  var dataLoading = false;

  var sourcesList = <String>[];
  var columnsList = <String>[];
  var joinColumns = <String>[];

  var editColumnLoading = false;
  var addColumnLoading = false;
  var deleteColumnLoading = false;
  var deleteIndexLoading = false;

  String? editColumnChoose;
  var editColumnValue = "";
  var addColumnValue = "";
  String? deleteColumnChoose;
  String? deleteSourceChoose;

  SettingView({Key? key}) : super(key: key);

  @override
  State<StatefulWidget> createState() => _SettingViewState();
}

class _SettingViewState extends State<SettingView> {
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

    /* Items tables names */

    var sourcesItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Clear all"),
    );

    sourcesItems.add(item);

    for (var e in widget.sourcesList) {
      item = DropdownMenuItem<String?>(
        value: e,
        child: Text(e),
      );

      sourcesItems.add(item);
    }

    /* Items app columns */

    var columnsItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Choose column"),
    );

    columnsItems.add(item);

    for (var e in widget.columnsList) {
      item = DropdownMenuItem<String?>(
        value: e,
        child: Text(e),
      );

      columnsItems.add(item);
    }

    /* Columns items 1*/

    var columnsMultiItems = <MultiSelectItem<String>>[];

    for (var e in widget.columnsList) {
      item = MultiSelectItem<String>(e, e);

      columnsMultiItems.add(item);
    }

    /* Sources items 1*/

    var sourcesMultiItems = <MultiSelectItem<String>>[];

    for (var e in widget.sourcesList) {
      item = MultiSelectItem<String>(e, e);

      sourcesMultiItems.add(item);
    }

    /* Build */

    return SingleChildScrollView(
        child: Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.spaceAround,
        crossAxisAlignment: CrossAxisAlignment.center,
        children: [
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
          /* Edit column */
          Container(
              width: Utilities.formWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                    key: GlobalKey<FormFieldState>(),
                    value: widget.editColumnChoose,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        labelText: "Edit Column"),
                    items: columnsItems,
                    onChanged: (value) {
                      widget.editColumnChoose = value;

                      /* update edit column text field */

                      setState(() {
                        widget.editColumnValue = value ?? "";
                      });
                    },
                  ),
                  TextFormField(
                    key: GlobalKey<FormFieldState>(),
                    initialValue: widget.editColumnValue,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        hintText: "Modified column"),
                    onChanged: (value) {
                      widget.editColumnValue = value;
                    },
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: widget.editColumnLoading
                        ? const CircularProgressIndicator()
                        : ElevatedButton(
                            onPressed: () {
                              String? oldColumnName = widget.editColumnChoose;

                              if (oldColumnName != null) {
                                String newColumnName = widget.editColumnValue;

                                saveAppColumn(oldColumnName, newColumnName);
                              } else {
                                log("Column name is null");
                              }
                            },
                            child: const Text("Edit"),
                          ),
                  )
                ],
              )),

          /* Add column */
          Container(
              width: Utilities.formWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  TextFormField(
                    key: GlobalKey<FormFieldState>(),
                    initialValue: widget.addColumnValue,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        hintText: "New column",
                        labelText: "Add Column"),
                    onChanged: (value) {
                      widget.addColumnValue = value;
                    },
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: widget.addColumnLoading
                        ? const CircularProgressIndicator()
                        : ElevatedButton(
                            onPressed: () {
                              String newColumnName = widget.addColumnValue;

                              saveAppColumn(null, newColumnName);
                            },
                            child: const Text("Add"),
                          ),
                  )
                ],
              )),

          /* Delete column */
          Container(
              width: Utilities.formWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                    key: GlobalKey<FormFieldState>(),
                    value: widget.deleteColumnChoose,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        labelText: "Delete Column"),
                    items: columnsItems,
                    onChanged: (value) {
                      widget.deleteColumnChoose = value;
                      log("$value");
                    },
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: widget.deleteColumnLoading
                        ? const CircularProgressIndicator()
                        : ElevatedButton(
                            onPressed: () {
                              String? columnName = widget.deleteColumnChoose;

                              if (columnName != null) {
                                deleteColumn(columnName);
                              } else {
                                log("Column name is null");
                              }
                            },
                            child: const Text("Delete"),
                          ),
                  )
                ],
              )),

          /* Delete table */

          Container(
              width: Utilities.formWidth,
              margin: const EdgeInsets.fromLTRB(0, 10, 0, 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                      key: GlobalKey<FormFieldState>(),
                      value: widget.deleteSourceChoose,
                      decoration: const InputDecoration(
                          filled: true,
                          fillColor: Utilities.fieldFillColor,
                          border: OutlineInputBorder(),
                          contentPadding: EdgeInsets.symmetric(horizontal: 10),
                          labelText: "Delete Index"),
                      items: sourcesItems,
                      onChanged: (value) {
                        widget.deleteSourceChoose = value;
                      }),
                  Align(
                      alignment: Alignment.centerRight,
                      child: widget.deleteIndexLoading
                          ? const CircularProgressIndicator()
                          : ElevatedButton(
                              onPressed: () {
                                String? tableName = widget.deleteSourceChoose;

                                // if (tableName != null) {
                                dropTable(tableName);
                                // } else {
                                //   log("value is null");
                                // }
                              },
                              child: const Text("Delete"),
                            ))
                ],
              )),

          /* Joins columns */

          Container(
              width: Utilities.formWidth,
              margin: const EdgeInsets.fromLTRB(0, 10, 0, 10),
              child: Column(children: [
                MultiSelectDialogField<String>(
                  key: GlobalKey<FormFieldState>(),
                  initialValue: widget.joinColumns,
                  searchHint: "Filters",
                  buttonText: const Text("Filters Columns"),
                  decoration: BoxDecoration(
                    border: Border.all(),
                  ),
                  listType: MultiSelectListType.LIST,
                  items: columnsMultiItems,
                  onConfirm: (value) {
                    widget.joinColumns.clear();
                    widget.joinColumns.addAll(value);

                    for (var e in widget.columnsList) {
                      /* if column exist in joinlist then update join value to true */

                      if (widget.joinColumns.contains(e)) {
                        updateJoin(e, true);
                      }

                      /* else update value to false */

                      else {
                        updateJoin(e, false);
                      }
                    }
                  },
                ),
              ])),

          /* Apply Join */

          Container(
              width: Utilities.formWidth,
              margin: const EdgeInsets.fromLTRB(0, 10, 0, 10),
              child: Column(children: [
                MultiSelectDialogField<String>(
                  key: GlobalKey<FormFieldState>(),
                  // initialValue: widget.joinSourcesToApply,
                  searchHint: "Join To Apply",
                  buttonText: const Text("Apply Join Source"),
                  decoration: BoxDecoration(
                    border: Border.all(),
                  ),
                  listType: MultiSelectListType.LIST,
                  items: sourcesMultiItems,
                  onConfirm: (value) {
                    // widget.joinSourcesToApply.clear();
                    // widget.joinSourcesToApply.addAll(value);

                    applyJoinSources(value);
                  },
                ),
              ])),
        ],
      ),
    ));
  }

  void loadTablesNames() {
    setState(() {
      widget.dataLoading = true;
    });

    widget._webAPIService.tablesImported().then((value) {
      widget.sourcesList.clear();

      // setState(() {
      widget.sourcesList.addAll(value);
      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.sourcesList.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void loadAppColumns() {
    setState(() {
      widget.dataLoading = true;
    });
    widget._webAPIService.appColumns().then((value) {
      widget.columnsList.clear();

      // setState(() {
      widget.columnsList.addAll(value);
      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.columnsList.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void loadJoinsColumns() {
    setState(() {
      widget.dataLoading = true;
    });
    widget._webAPIService.allJoins().then((value) {
      widget.joinColumns.clear();

      // setState(() {
      widget.joinColumns.addAll(value);
      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.joinColumns.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void saveAppColumn(String? oldColumnName, String newColumnName) {
    setState(() {
      widget.editColumnLoading = true;
      widget.addColumnLoading = true;
      widget.deleteColumnLoading = true;
    });
    WebAPIService().updateAppColumn(oldColumnName, newColumnName).then((_) {
      var snackBar = const SnackBar(content: Text("Successful operation"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);

      /* refresh data */

      widget.editColumnChoose = null;
      widget.editColumnValue = "";
      widget.addColumnValue = ""; // text field in case of new creation
      loadAppColumns();
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);

      var snackBar = SnackBar(content: Text("${error?.toString()}"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    }).whenComplete(() {
      setState(() {
        widget.editColumnLoading = false;
        widget.addColumnLoading = false;
        widget.deleteColumnLoading = false;
      });
    });
  }

  void deleteColumn(String columnName) {
    setState(() {
      widget.editColumnLoading = true;
      widget.addColumnLoading = true;
      widget.deleteColumnLoading = true;
    });
    WebAPIService().deleteColumn(columnName).then((_) {
      var snackBar = const SnackBar(content: Text("Successful operation"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);

      /* refresh data */

      widget.editColumnValue = "";
      widget.editColumnChoose = null;
      widget.addColumnValue = "";
      widget.deleteColumnChoose = null;
      loadAppColumns();
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);

      var snackBar = SnackBar(content: Text("${error?.toString()}"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    }).whenComplete(() {
      setState(() {
        widget.editColumnLoading = false;
        widget.addColumnLoading = false;
        widget.deleteColumnLoading = false;
      });
    });
  }

  void updateJoin(String columnName, bool value) {
    WebAPIService().updateJoin(columnName, value).then((_) {
      /* refresh data */

      loadJoinsColumns();
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);

      var snackBar = SnackBar(content: Text("${error?.toString()}"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    });
  }

  void dropTable(String? tableName) {
    setState(() {
      widget.deleteIndexLoading = true;
    });
    WebAPIService().dropTable(tableName).then((_) {
      var snackBar = const SnackBar(content: Text("Successful operation"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);

      /* reset form */

      widget.deleteSourceChoose = null;
      loadTablesNames();
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);

      var snackBar = SnackBar(content: Text("${error?.toString()}"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    }).whenComplete(() {
      setState(() {
        widget.deleteIndexLoading = false;
      });
    });
  }

  /// generate and show Join Dialog
  void showJoinForm() {
    var joinsValues = <String, bool>{};

    for (var col in widget.columnsList) {
      var value = widget.joinColumns.contains(col);
      joinsValues.putIfAbsent(col, () => value);
    }

    showDialog(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text("Join"),
          content: StatefulBuilder(builder: (context1, state) {
            var joinsRows = <Widget>[];

            for (var col in joinsValues.keys) {
              var value = joinsValues[col];

              joinsRows.add(CheckboxListTile(
                  title: Text(col),
                  value: value,
                  onChanged: (v) {
                    if (v != null) {
                      joinsValues[col] = v;

                      state(() {
                        value = v;
                      });
                    }
                  }));
            }

            return SingleChildScrollView(
              child: Column(
                mainAxisAlignment: MainAxisAlignment.spaceAround,
                children: joinsRows,
              ),
            );
          }),
          actions: <Widget>[
            TextButton(
              child: const Text('Save'),
              onPressed: () {
                for (var e in joinsValues.keys) {
                  var value = joinsValues[e]!;

                  /* if join exist and value is now false then remove join */

                  if (widget.joinColumns.contains(e) && !value) {
                    updateJoin(e, false);
                  }

                  /* else if join not exist and value is now true then add join */

                  else if (!widget.joinColumns.contains(e) && value) {
                    updateJoin(e, true);
                  }
                }

                Navigator.of(context).pop();
              },
            ),
            TextButton(
              child: const Text('Cancel'),
              onPressed: () {
                Navigator.of(context).pop();
              },
            ),
          ],
        );
      },
    );
  }

  void refreshData() {
    loadTablesNames();
    loadAppColumns();
    loadJoinsColumns();
  }

  void applyJoinSources(List<String?> value) {
    setState(() {
      widget.dataLoading = true;
    });

    WebAPIService().applyJoinSources(value).then((_) {
      showDialog(
          context: context,
          builder: (context) {
            return const AlertDialog(
                content: SelectableText(
                    "Operation initiated, please check Status form"));
          });
    }).catchError((error, stackTrace) {
      log(error.toString(), error: error, stackTrace: stackTrace);

      showDialog(
          context: context,
          builder: (context) {
            return AlertDialog(
                title: const Text("Error"),
                content: SelectableText(error.toString()));
          });
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }
}
