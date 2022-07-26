import 'dart:async';
import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';

class ImportView extends StatefulWidget {
  ImportView({Key? key}) : super(key: key);

  final _webAPIService = WebAPIService();
  // final _utilities = Utilities();

  final _dropdownFilesNamesKey = GlobalKey<FormFieldState>();
  final _dropdownTablesNamesKey = GlobalKey<FormFieldState>();
  final _textFieldCSVNameKey = GlobalKey<FormFieldState>();
  final _dropdownDelimiterKey = GlobalKey<FormFieldState>();

  final _dropDownColumnToMapKeys = <GlobalKey<FormFieldState>>[];
  final _textFieldNewColumnNameKeys = <GlobalKey<FormFieldState>>[];

  var dataInitialized = false;
  var dataLoading = false;
  var excludeHeader = false;
  var allowImport = false;

  var filesNames = <String>[];
  var tablesNames = <String>[];
  // var fileStructure = <String>[];
  var appColumns = <String>[];

  var dataStruct = Completer<Map<String, dynamic>>();
  Widget? filePreviewWidget;

  @override
  State<StatefulWidget> createState() => _ImportViewState();
}

class _ImportViewState extends State<ImportView> {
  @override
  void didChangeDependencies() {
    super.didChangeDependencies();

    if (!widget.dataInitialized) {
      widget.filePreviewWidget = const SizedBox.shrink();
      widget.dataStruct.complete(<String, dynamic>{});

      refreshData();
      widget.dataInitialized = true;
    }
  }

  @override
  Widget build(BuildContext context) {
    dynamic item;

    /* Files to import Items */

    var filesNamesItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Choose File"),
    );

    filesNamesItems.add(item);

    for (var e in widget.filesNames) {
      var fileName = e.split('/').last; // get short file name

      item = DropdownMenuItem(
        value: e,
        child: Text(fileName),
      );

      filesNamesItems.add(item);
    }

    /* Tables names to append Items */

    var tablesNamesItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Other"),
    );

    tablesNamesItems.add(item);

    for (var e in widget.tablesNames) {
      item = DropdownMenuItem<String?>(
        value: e,
        child: Text(e),
      );

      tablesNamesItems.add(item);
    }

    /* delimiters Items */

    var delimitersItems = const <DropdownMenuItem<String>>[
      DropdownMenuItem(value: ",", child: Text(",")),
      DropdownMenuItem(value: ";", child: Text(";")),
      DropdownMenuItem(value: ":", child: Text(":")),
      DropdownMenuItem(value: "|", child: Text("|"))
    ];

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
          /* Import file */
          Container(
              width: Utilities.fieldWidth,
              // height: Utilities.fieldHeight,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Column(
                children: [
                  DropdownButtonFormField<String?>(
                    key: widget._dropdownFilesNamesKey,
                    style: Utilities.itemStyle,
                    icon: const Icon(Icons.search),
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        labelText: "File Name"),
                    items: filesNamesItems,
                    onChanged: (value) {
                      widget._dropdownTablesNamesKey.currentState?.reset();
                      widget._textFieldCSVNameKey.currentState?.reset();

                      if (value != null) {
                        var fileName = value.split('/').last; // short file name
                        widget._textFieldCSVNameKey.currentState
                            ?.didChange(fileName);
                      }
                      /* clear file structure preview */
                      setState(() {
                        widget.filePreviewWidget = const SizedBox.shrink();
                        widget.dataStruct = Completer<Map<String, dynamic>>();
                      });
                      widget.dataStruct.complete(<String, dynamic>{});
                    },
                  ),
                  DropdownButtonFormField<String?>(
                      key: widget._dropdownTablesNamesKey,
                      style: Utilities.itemStyle,
                      decoration: const InputDecoration(
                          filled: true,
                          fillColor: Utilities.fieldFillColor,
                          border: OutlineInputBorder(),
                          contentPadding: EdgeInsets.symmetric(horizontal: 10),
                          labelText: "Index"),
                      items: tablesNamesItems,
                      onChanged: (value) {}),
                  TextFormField(
                    key: widget._textFieldCSVNameKey,
                    style: Utilities.itemStyle,
                    decoration: const InputDecoration(
                        filled: true,
                        fillColor: Utilities.fieldFillColor,
                        border: OutlineInputBorder(),
                        contentPadding: EdgeInsets.symmetric(horizontal: 10),
                        labelText: "If other",
                        hintText: "CSV Name"),
                  ),
                  DropdownButtonFormField<String>(
                      key: widget._dropdownDelimiterKey,
                      style: Utilities.itemStyle,
                      value: ",",
                      decoration: const InputDecoration(
                          filled: true,
                          fillColor: Utilities.fieldFillColor,
                          border: OutlineInputBorder(),
                          contentPadding: EdgeInsets.symmetric(horizontal: 10),
                          labelText: "Delimiter"),
                      items: delimitersItems,
                      onChanged: (value) {}),
                  CheckboxListTile(
                      title: const Text("Exclude header"),
                      value: widget.excludeHeader,
                      onChanged: (value) {
                        if (value != null) {
                          setState(() {
                            widget.excludeHeader = value;
                            /* clear file structure preview */
                            widget.filePreviewWidget = const SizedBox.shrink();
                            widget.dataStruct =
                                Completer<Map<String, dynamic>>();
                          });

                          widget.dataStruct.complete(<String, dynamic>{});
                        }
                      }),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceAround,
                    children: [
                      ElevatedButton(
                          onPressed: () {
                            String? filePath = widget
                                ._dropdownFilesNamesKey.currentState?.value;

                            if (filePath != null) {
                              loadFileStructure(filePath);
                            } else {
                              log("file path is null");
                            }
                          },
                          child: const Text(
                            "Preview",
                            style: Utilities.itemStyle,
                          )),
                      ElevatedButton(
                          onPressed: widget.allowImport
                              ? () {
                                  runFileImportation();
                                }
                              : null,
                          child: const Text(
                            "Import",
                            style: Utilities.itemStyle,
                          ))
                    ],
                  )
                ],
              )),
          /* Preview */
          FutureBuilder<Map<String, dynamic>>(
              future: widget.dataStruct.future,
              builder: ((context, snapshot) {
                if (snapshot.connectionState == ConnectionState.done &&
                    snapshot.hasData) {
                  // var data = snapshot.requireData;

                  return widget.filePreviewWidget ??
                      const Center(
                          child: Text(
                              "No data available. Please press preview button"));
                } else if (snapshot.connectionState ==
                    ConnectionState.waiting) {
                  return const Center(
                    child: CircularProgressIndicator(),
                  );
                } else {
                  if (snapshot.hasError) {
                    var error = snapshot.error;
                    log("${error?.toString()}",
                        error: error, stackTrace: snapshot.stackTrace);
                  }

                  return const Center(
                      child: Text(
                          "No data available. Please press refresh button"));
                }
              }))
        ],
      ),
    ));
  }

  void loadFilesNames() {
    setState(() {
      widget.dataLoading = true;
    });

    widget._webAPIService.filesToImport().then((value) {
      widget.filesNames.clear();
      // widget.fileStructure.clear();
      widget.filePreviewWidget = const SizedBox.shrink();
      widget.dataStruct = Completer<Map<String, dynamic>>();
      widget.dataStruct.complete(<String, dynamic>{});

      // setState(() {
      widget.filesNames.addAll(value);

      // });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.filesNames.clear();
      widget.filePreviewWidget = const SizedBox.shrink();
      widget.dataStruct = Completer<Map<String, dynamic>>();
      widget.dataStruct.complete(<String, dynamic>{});
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
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

  void loadFileStructure(String filePath) {
    setState(() {
      widget.dataStruct = Completer<Map<String, dynamic>>();
      widget.allowImport = false;
    });

    String? delimiter = widget._dropdownDelimiterKey.currentState?.value;

    widget._webAPIService
            .fileStructure(filePath, widget.excludeHeader, delimiter)
            .then((value) {
      /* Columns mapping Items */

      var columnsMapItems = <DropdownMenuItem<String?>>[];

      var item = const DropdownMenuItem<String?>(
        value: null,
        child: Text("New or Delete"),
      );

      columnsMapItems.add(item);

      for (var e in widget.appColumns) {
        var item = DropdownMenuItem<String?>(
          value: e,
          child: Text(e),
        );

        columnsMapItems.add(item);
      }
      // setState(() {
      widget.filePreviewWidget = filePreview(value, columnsMapItems);
      widget.dataStruct.complete(value);
      // });

      setState(() {
        widget.allowImport = true;
      });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.filePreviewWidget = const SizedBox.shrink();
      widget.dataStruct.completeError(error, stackTrace);

      setState(() {
        widget.allowImport = false;
      });
    })
        // .whenComplete(() {
        //   setState(() {});
        // })
        ;
  }

  void importFile(String fileName, String tableName,
      Map<String, String?> fileNewStructure) {
    setState(() {
      widget.dataLoading = true;
    });

    String? delimiter = widget._dropdownDelimiterKey.currentState?.value;

    WebAPIService()
        .importFile(fileName, tableName, fileNewStructure, widget.excludeHeader,
            delimiter)
        .then((_) {
      var snackBar = const SnackBar(
          content: Text("Operation initiated, please check Status form"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);

      /* reset form */

      widget._dropdownFilesNamesKey.currentState?.reset();
      widget._dropdownTablesNamesKey.currentState?.reset();
      widget._dropdownDelimiterKey.currentState?.reset();
      widget._textFieldCSVNameKey.currentState?.reset();
      widget.excludeHeader = false;
      widget.allowImport = false;

      loadFilesNames();
      loadTablesNames();
      loadAppColumns();
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);

      var snackBar = SnackBar(content: Text("${error?.toString()}"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void refreshData() {
    loadFilesNames();
    loadTablesNames();
    loadAppColumns();
  }

  void runFileImportation() async {
    String? fileName = widget._dropdownFilesNamesKey.currentState?.value;

    /* get table name directly on table list else if null then on csv name text field */
    String? tableName = widget._dropdownTablesNamesKey.currentState?.value;
    tableName ??= widget._textFieldCSVNameKey.currentState?.value;

    /* generate structure new file to import */

    var fileNewStructure =
        <String, String?>{}; //default column name, mapped column

    if (widget.dataStruct.isCompleted) {
      var data = await widget.dataStruct.future;
      List<String> structure = data.containsKey(WebAPIService.fileStructureKey)
          ? data[WebAPIService.fileStructureKey]
          : [];

      for (int i = 0; i < structure.length; i++) {
        String? mapColumnName =
            widget._dropDownColumnToMapKeys[i].currentState?.value;
        String? newColumnName =
            widget._textFieldNewColumnNameKeys[i].currentState?.value;

        /* if column mapped is null then take new column name*/

        if (mapColumnName == null) {
          if (newColumnName != null && newColumnName.trim().isNotEmpty) {
            fileNewStructure.putIfAbsent(structure[i], () => newColumnName);
          } else {
            fileNewStructure.putIfAbsent(structure[i],
                () => null); // null means columns value must be ignored
          }
        } else {
          fileNewStructure.putIfAbsent(structure[i], () => mapColumnName);
        }
      }

      if (fileName != null &&
          (tableName != null && tableName.trim().isNotEmpty)) {
        importFile(fileName, tableName, fileNewStructure);
      } else {
        log("File name or Table name is null");
      }
    } else {
      log("data stucture is not loaded");
    }
  }

  Widget filePreview(Map<String, dynamic> data,
      List<DropdownMenuItem<String?>> columnsMapItems) {
    List<String> structure = data.containsKey(WebAPIService.fileStructureKey)
        ? data[WebAPIService.fileStructureKey]
        : [];
    List<List<String>> preview = data.containsKey(WebAPIService.filePreviewKey)
        ? data[WebAPIService.filePreviewKey]
        : [];

    if (structure.isNotEmpty) {
      var rows = <Row>[];
      var columnsNames = <Widget>[];
      var columnToMapCells = <Widget>[]; // choose column to map to schema
      var newColumnNameCells =
          <Widget>[]; // or put directly new column name, empty to ignore

      widget._dropDownColumnToMapKeys.clear();
      widget._textFieldNewColumnNameKeys.clear();

      for (int i = 0; i < structure.length; i++) {
        /* Columns names */

        var colStrucName = structure[i];

        columnsNames.add(Container(
            width: Utilities.fieldWidth,
            // height: Utilities.fieldHeight,
            margin: const EdgeInsets.symmetric(vertical: 10),
            child: SelectableText(colStrucName)));

        /* Prepare column to map to schema */

        widget._dropDownColumnToMapKeys.add(GlobalKey<FormFieldState>());

        columnToMapCells.add(Container(
            width: Utilities.fieldWidth,
            // height: Utilities.fieldHeight,
            margin: const EdgeInsets.fromLTRB(0, 10, 0, 10),
            child: DropdownButtonFormField<String?>(
                key: widget._dropDownColumnToMapKeys[i],
                style: Utilities.itemStyle,
                decoration: const InputDecoration(
                    filled: true,
                    fillColor: Utilities.fieldFillColor,
                    border: OutlineInputBorder(),
                    contentPadding: EdgeInsets.symmetric(horizontal: 10),
                    labelText: "Map column"),
                items: columnsMapItems,
                value:
                    i >= widget.appColumns.length ? null : widget.appColumns[i],
                onChanged: (value) {})));

        /* Prepare new column name, empty to ignore */

        widget._textFieldNewColumnNameKeys.add(GlobalKey<FormFieldState>());
        // var initialValue =
        //     i >= widget.appColumns.length ? "Column$i" : widget.appColumns[i];
        String? initialValue;

        newColumnNameCells.add(Container(
            width: Utilities.fieldWidth,
            // height: Utilities.fieldHeight,
            margin: const EdgeInsets.symmetric(vertical: 10),
            child: TextFormField(
              key: widget._textFieldNewColumnNameKeys[i],
              style: Utilities.itemStyle,
              decoration: const InputDecoration(
                  filled: true,
                  fillColor: Utilities.fieldFillColor,
                  border: OutlineInputBorder(),
                  contentPadding: EdgeInsets.symmetric(horizontal: 10),
                  labelText: "Empty to ignore",
                  hintText: "Column Name"),
              initialValue: initialValue,
            )));
      }

      /* apply structure */

      rows.add(Row(children: columnsNames));

      rows.add(Row(children: columnToMapCells));

      rows.add(Row(children: newColumnNameCells));

      /* apply preview data */

      for (var row in preview) {
        var rowData = <Widget>[];

        for (int i = 0; i < structure.length; i++) {
          var cellValue = Utilities.defaultCellEmptyValue;

          if (i < row.length) {
            cellValue = row[i];
          }

          rowData.add(Container(
              width: Utilities.fieldWidth,
              // height: Utilities.fieldHeight,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: SelectableText(cellValue)));
        }

        rows.add(Row(children: rowData));
      }

      var x = ScrollController();

      return Column(
        children: [
          Scrollbar(
              // scrollbarOrientation: ScrollbarOrientation.right,
              controller: x,
              trackVisibility: true,
              child: SingleChildScrollView(
                scrollDirection: Axis.horizontal,
                // physics: const AlwaysScrollableScrollPhysics(),
                controller: x,
                padding: const EdgeInsets.symmetric(horizontal: 10),
                child: Column(
                  children: rows,
                ),
              )),
          // Align(
          //   alignment: Alignment.centerRight,
          //   child: ElevatedButton(
          //       onPressed:
          //           //  widget.dataStruct.isCompleted
          //           //     ?
          //           () {
          //         runFileImportation();
          //       }
          //       // : null
          //       ,
          //       child: const Text(
          //         "Import",
          //         style: Utilities.itemStyle,
          //       )),
          // )
        ],
      );
    } else {
      return const Center(
          child: Text("No data available. Please press Preview button"));
    }
  }
}
