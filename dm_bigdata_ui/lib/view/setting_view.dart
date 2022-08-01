import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';

class SettingView extends StatefulWidget {
  final _webAPIService = WebAPIService();
  final _textFieldEditColumnKey = GlobalKey<FormFieldState>();
  final _dropdownEditColumnKey = GlobalKey<FormFieldState>();
  final _textFieldAddColumnKey = GlobalKey<FormFieldState>();
  final _dropdownDeleteColumnKey = GlobalKey<FormFieldState>();
  final _dropdownDeleteTableKey = GlobalKey<FormFieldState>();

  var dataInitialized = false;
  var dataLoading = false;

  var tablesNames = <String>[];
  var appColumns = <String>[];
  var joinColumns = <String>[];

  var editColumnLoading = false;
  var addColumnLoading = false;
  var deleteColumnLoading = false;
  var deleteIndexLoading = false;

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

    var tablesNamesItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Clear all"),
    );

    tablesNamesItems.add(item);

    // for (var e in widget.tablesNames) {
    //   item = DropdownMenuItem<String?>(
    //     value: e,
    //     child: Text(e),
    //   );

    //   tablesNamesItems.add(item);
    // }

    /* Items app columns */

    var appColumnsItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Choose column"),
    );

    appColumnsItems.add(item);

    for (var e in widget.appColumns) {
      item = DropdownMenuItem<String?>(
        value: e,
        child: Text(e),
      );

      appColumnsItems.add(item);
    }

    /* Build */

    return Center(
        child: SingleChildScrollView(
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
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                    key: widget._dropdownEditColumnKey,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        labelText: "Edit Column"),
                    items: appColumnsItems,
                    onChanged: (value) {
                      /* update edit column text field */

                      if (value != null) {
                        widget._textFieldEditColumnKey.currentState!
                            .didChange(value);
                      } else {
                        widget._textFieldEditColumnKey.currentState!.reset();
                      }
                    },
                  ),
                  TextFormField(
                    key: widget._textFieldEditColumnKey,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        hintText: "Modified column"),
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: widget.editColumnLoading
                        ? const CircularProgressIndicator()
                        : ElevatedButton(
                            onPressed: () {
                              String? oldColumnName = widget
                                  ._dropdownEditColumnKey.currentState?.value;

                              if (oldColumnName != null) {
                                String newColumnName = widget
                                    ._textFieldEditColumnKey
                                    .currentState
                                    ?.value;

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
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  TextFormField(
                    key: widget._textFieldAddColumnKey,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        hintText: "New column",
                        labelText: "Add Column"),
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: widget.addColumnLoading
                        ? const CircularProgressIndicator()
                        : ElevatedButton(
                            onPressed: () {
                              String newColumnName = widget
                                  ._textFieldAddColumnKey.currentState?.value;

                              saveAppColumn(null, newColumnName);
                            },
                            child: const Text("Add"),
                          ),
                  )
                ],
              )),

          /* Delete column */
          Container(
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                    key: widget._dropdownDeleteColumnKey,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        labelText: "Delete Column"),
                    items: appColumnsItems,
                    onChanged: (value) {
                      log("$value");
                    },
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: widget.deleteColumnLoading
                        ? const CircularProgressIndicator()
                        : ElevatedButton(
                            onPressed: () {
                              String? columnName = widget
                                  ._dropdownDeleteColumnKey.currentState?.value;

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
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.fromLTRB(0, 10, 0, 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                      key: widget._dropdownDeleteTableKey,
                      decoration: const InputDecoration(
                          filled: true,
                          fillColor: Utilities.fieldFillColor,
                          border: OutlineInputBorder(),
                          contentPadding: EdgeInsets.symmetric(horizontal: 10),
                          labelText: "Delete Index"),
                      items: tablesNamesItems,
                      onChanged: (value) {}),
                  Align(
                      alignment: Alignment.centerRight,
                      child: widget.deleteIndexLoading
                          ? const CircularProgressIndicator()
                          : ElevatedButton(
                              onPressed: () {
                                String? tableName = widget
                                    ._dropdownDeleteTableKey
                                    .currentState
                                    ?.value;

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
          ElevatedButton(
            onPressed: () {
              showJoinForm();
            },
            child: const Text("Joincture"),
          ),
        ],
      ),
    ));
  }

  void loadTablesNames() {
    setState(() {
      widget.dataLoading = true;
    });

    widget._webAPIService.tablesImported().then((value) {
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

  void loadAppColumns() {
    setState(() {
      widget.dataLoading = true;
    });
    widget._webAPIService.appColumns().then((value) {
      widget.appColumns.clear();

      // setState(() {
      widget.appColumns.addAll(value);
      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.appColumns.clear();
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

      widget._dropdownEditColumnKey.currentState?.reset();
      widget._textFieldEditColumnKey.currentState?.reset();
      widget._textFieldAddColumnKey.currentState
          ?.reset(); // text field in case of new creation
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

      widget._textFieldEditColumnKey.currentState?.reset();
      widget._dropdownEditColumnKey.currentState?.reset();
      widget._textFieldAddColumnKey.currentState?.reset();
      widget._dropdownDeleteColumnKey.currentState?.reset();
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

      widget._dropdownDeleteTableKey.currentState?.reset();
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

    for (var col in widget.appColumns) {
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
}
