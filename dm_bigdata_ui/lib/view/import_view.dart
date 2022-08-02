import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';

class ImportView extends StatefulWidget {
  ImportView({Key? key}) : super(key: key);

  final _webAPIService = WebAPIService();
  final _importFormKey = GlobalKey<FormState>();
  static final delimitersList = [",", ";", ":", "|"];

  String? source;
  String? newSource;
  var delimiter = ImportView.delimitersList.first;
  var excludeHeader = false;
  String? importPath;
  String? fileChosenName;
  Map<String, dynamic>? filePreviewData;

  var columnsMap = <String?>[];
  var newColumnsNames = <String>[];

  var dataInitialized = false;
  var dataLoading = false;
  var sourcesList = <String>[];
  var columnsList = <String>[];

  @override
  State<StatefulWidget> createState() => _ImportViewState();
}

class _ImportViewState extends State<ImportView> {
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

    /* sources Items */

    var sourcesItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("Other"),
    );

    sourcesItems.add(item);

    for (var e in widget.sourcesList) {
      item = DropdownMenuItem<String?>(
        value: e,
        child: Text(e),
      );

      sourcesItems.add(item);
    }

    /* delimiters Items */

    var delimitersItems = const <DropdownMenuItem<String>>[
      DropdownMenuItem(value: ",", child: Text(",")),
      DropdownMenuItem(value: ";", child: Text(";")),
      DropdownMenuItem(value: ":", child: Text(":")),
      DropdownMenuItem(value: "|", child: Text("|"))
    ];

    /* columns mapping Items */

    var columnsMapItems = <DropdownMenuItem<String?>>[];

    item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("New or Delete"),
    );

    columnsMapItems.add(item);

    for (var e in widget.columnsList) {
      var item = DropdownMenuItem<String?>(
        value: e,
        child: Text(e),
      );

      columnsMapItems.add(item);
    }

    /* Build */

    return SingleChildScrollView(
        child: Center(
      child: Column(
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
              width: Utilities.formWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: Form(
                  key: widget._importFormKey,
                  child: Column(
                    children: [
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceAround,
                        children: [
                          ElevatedButton(
                              onPressed: () async {
                                var filePickerResult = await FilePicker.platform
                                    .pickFiles(withReadStream: true);

                                if (filePickerResult != null) {
                                  /* star load file to server */
                                  var file = filePickerResult.files.single;

                                  setState(() {
                                    widget.dataLoading = true;
                                  });

                                  widget._webAPIService.uploadFile(file,
                                      onProgress: ((sentBytes, totalBytes) {
                                    var progress =
                                        (sentBytes * 100) ~/ totalBytes;

                                    setState(() {
                                      widget.fileChosenName = "$progress %";
                                    });
                                  })).then((value) {
                                    // setState(() {
                                    widget.newSource = file.name;
                                    widget.fileChosenName = file.name;
                                    widget.importPath = value;
                                    // });
                                  }).catchError((error, stackTrace) {
                                    var errorMsg = "${error?.toString()}";

                                    widget.fileChosenName = null;

                                    log(errorMsg, stackTrace: stackTrace);

                                    widget.importPath = null;

                                    showDialog(
                                        context: context,
                                        builder: (context) {
                                          return AlertDialog(
                                              title: const Text("Error"),
                                              content:
                                                  SelectableText(errorMsg));
                                        });
                                  }).whenComplete(() {
                                    setState(() {
                                      widget.dataLoading = false;
                                    });
                                  });
                                }
                              },
                              child: const Text("File name")),
                          Text(widget.fileChosenName ?? "No file chosen")
                        ],
                      ),
                      Padding(
                        padding: const EdgeInsets.symmetric(
                          vertical: 10,
                        ),
                        child: DropdownButtonFormField<String?>(
                          key: GlobalKey(),
                          value: widget.source,
                          decoration: const InputDecoration(
                              border: OutlineInputBorder(), labelText: "Index"),
                          items: sourcesItems,
                          onChanged: (value) {
                            widget.source = value;
                          },
                          onSaved: (value) {
                            widget.source = value;
                          },
                        ),
                      ),
                      Padding(
                        padding: const EdgeInsets.symmetric(vertical: 10),
                        child: TextFormField(
                          key: GlobalKey(),
                          initialValue: widget.newSource,
                          decoration: const InputDecoration(
                              border: OutlineInputBorder(),
                              labelText: "If other",
                              hintText: "Source name"),
                          validator: (value) {
                            if (widget.source != null) {
                              return null;
                            } else {
                              if (value != null && value.trim().isNotEmpty) {
                                return null;
                              } else {
                                return "This value is mandatory";
                              }
                            }
                          },
                          onSaved: (value) {
                            widget.newSource = value;
                          },
                          onChanged: (value) {
                            widget.newSource = value;
                          },
                        ),
                      ),
                      Padding(
                        padding: const EdgeInsets.symmetric(vertical: 10),
                        child: DropdownButtonFormField<String>(
                          key: GlobalKey(),
                          value: widget.delimiter,
                          decoration: const InputDecoration(
                              border: OutlineInputBorder(),
                              labelText: "Delimiter"),
                          items: delimitersItems,
                          onChanged: (value) {
                            if (value != null) {
                              widget.delimiter = value;
                            }
                          },
                          onSaved: (value) {
                            if (value != null) {
                              widget.delimiter = value;
                            }
                          },
                        ),
                      ),
                      CheckboxListTile(
                          title: const Text("Exclude header"),
                          value: widget.excludeHeader,
                          onChanged: (value) {
                            if (value != null) {
                              setState(() {
                                widget.excludeHeader = value;
                              });
                            }
                          }),
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceAround,
                        children: [
                          ElevatedButton(
                              onPressed: () {
                                if ((widget._importFormKey.currentState
                                            ?.validate() ??
                                        false) &&
                                    widget.importPath != null) {
                                  widget._importFormKey.currentState?.save();

                                  loadPreviewFile();
                                } else {
                                  showDialog(
                                      context: context,
                                      builder: (context) {
                                        return const AlertDialog(
                                            title: Text("Error"),
                                            content: SelectableText(
                                                "Please complete and check input data"));
                                      });
                                }
                              },
                              child: const Text(
                                "Preview",
                              )),
                          ElevatedButton(
                              onPressed: widget.filePreviewData != null
                                  ? () {
                                      runFileImportation();
                                    }
                                  : null,
                              child: const Text(
                                "Import",
                              ))
                        ],
                      )
                    ],
                  ))),
          /* Preview */
          generateFilePreviewWidget(columnsMapItems)
        ],
      ),
    ));
  }

  void loadSources() {
    setState(() {
      widget.dataLoading = true;
    });
    widget._webAPIService.tablesImported().then((value) {
      widget.sourcesList.clear();
      widget.sourcesList.addAll(value);
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.sourcesList.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void loadColumns() {
    setState(() {
      widget.dataLoading = true;
    });
    widget._webAPIService.appColumns().then((value) {
      widget.columnsList.clear();
      widget.columnsList.addAll(value);
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.columnsList.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void loadPreviewFile() {
    setState(() {
      widget.dataLoading = true;
    });

    widget._webAPIService
        .loadPreviewFile(
            widget.importPath!, widget.excludeHeader, widget.delimiter)
        .then((value) {
      List<String> structure = value.containsKey(WebAPIService.fileStructureKey)
          ? value[WebAPIService.fileStructureKey]
          : [];

      widget.columnsMap.clear();
      widget.newColumnsNames.clear();

      for (int i = 0; i < structure.length; i++) {
        var e = i < widget.columnsList.length ? widget.columnsList[i] : null;

        widget.columnsMap.add(e);
        widget.newColumnsNames.add(e ?? "");
      }

      // setState(() {
      widget.filePreviewData = value;
//  });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void refreshData() {
    widget.source = null;
    widget.newSource = null;
    widget.delimiter = ImportView.delimitersList.first;
    widget.excludeHeader = false;
    widget.importPath = null;
    widget.filePreviewData = null;
    widget.fileChosenName = null;

    widget.columnsMap.clear();
    widget.newColumnsNames.clear();

    widget.dataInitialized = false;
    widget.dataLoading = false;

    loadSources();
    loadColumns();
  }

  void runFileImportation() async {
    try {
      setState(() {
        widget.dataLoading = true;
      });

      var data = widget.filePreviewData;

      if (data != null) {
        /* generate structure columns mapping */

        List<String> structure =
            data.containsKey(WebAPIService.fileStructureKey)
                ? data[WebAPIService.fileStructureKey]
                : [];

        var structureColumnsMapping = <String,
            String?>{}; // key => current file column, value => column to map after import, null to ignore column

        for (int i = 0; i < structure.length; i++) {
          String? columnMap = widget.columnsMap[i];
          String? newColumnName = widget.newColumnsNames[i];

          /* if column mapped is null then take new column name*/

          if (columnMap == null) {
            if (newColumnName != null && newColumnName.trim().isNotEmpty) {
              structureColumnsMapping.putIfAbsent(
                  structure[i], () => newColumnName);
            } else {
              structureColumnsMapping.putIfAbsent(structure[i],
                  () => null); // null means columns value must be ignored
            }
          } else {
            structureColumnsMapping.putIfAbsent(structure[i], () => columnMap);
          }
        }

        /* run import */

        await WebAPIService().importFile(
            widget.importPath,
            widget.source ?? widget.newSource,
            structureColumnsMapping,
            widget.excludeHeader,
            widget.delimiter);

        refreshData();

        showDialog(
            context: context,
            builder: (context) {
              return const AlertDialog(
                  content: SelectableText(
                      "Operation initiated, please check Status form"));
            });
      } else {
        throw Exception("data stucture is not loaded");
      }
    } catch (error, stackTrace) {
      log(error.toString(), error: error, stackTrace: stackTrace);

      showDialog(
          context: context,
          builder: (context) {
            return AlertDialog(
                title: const Text("Error"),
                content: SelectableText(error.toString()));
          });
    } finally {
      setState(() {
        widget.dataLoading = false;
      });
    }
  }

  Widget generateFilePreviewWidget(
      List<DropdownMenuItem<String?>> columnsMapItems) {
    var data = widget.filePreviewData;

    if (data != null) {
      List<String> structure = data.containsKey(WebAPIService.fileStructureKey)
          ? data[WebAPIService.fileStructureKey]
          : [];
      List<List<String>> preview =
          data.containsKey(WebAPIService.filePreviewKey)
              ? data[WebAPIService.filePreviewKey]
              : [];

      if (structure.isNotEmpty) {
        var rows = <Row>[];
        var columnsTitles = <Widget>[];
        var columnsMapCells = <Widget>[]; // choose column to map to schema
        var newColumnsNamesCells =
            <Widget>[]; // or put directly new column name, empty to ignore

        for (int i = 0; i < structure.length; i++) {
          /* Columns names */

          var colStrucName = structure[i];

          columnsTitles.add(Container(
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: SelectableText(colStrucName)));

          /* Prepare column to map to schema */

          columnsMapCells.add(Container(
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.fromLTRB(0, 10, 0, 10),
              child: DropdownButtonFormField<String?>(
                  key: GlobalKey(),
                  value: widget.columnsMap[i],
                  decoration: const InputDecoration(
                      border: OutlineInputBorder(), labelText: "Map column"),
                  items: columnsMapItems,
                  onChanged: (value) {
                    widget.columnsMap[i] = value;

                    /* update new column name */

                    setState(() {
                      if (value == null) {
                        widget.newColumnsNames[i] = "";
                      } else {
                        widget.newColumnsNames[i] = value;
                      }
                    });
                  })));

          /* Prepare new column name, empty to ignore */

          newColumnsNamesCells.add(Container(
              width: Utilities.fieldWidth,
              margin: const EdgeInsets.symmetric(vertical: 10),
              child: TextFormField(
                  key: GlobalKey(),
                  initialValue: widget.newColumnsNames[i],
                  decoration: const InputDecoration(
                      border: OutlineInputBorder(),
                      labelText: "Empty to ignore",
                      hintText: "Column Name"),
                  onChanged: (value) {
                    widget.newColumnsNames[i] = value;
                  })));
        }

        /* apply structure */

        rows.add(Row(children: columnsTitles));
        rows.add(Row(children: columnsMapCells));
        rows.add(Row(children: newColumnsNamesCells));

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
                margin: const EdgeInsets.symmetric(vertical: 10),
                child: SelectableText(cellValue)));
          }

          rows.add(Row(children: rowData));
        }

        var x = ScrollController();

        return Column(
          children: [
            Scrollbar(
                controller: x,
                trackVisibility: true,
                child: SingleChildScrollView(
                  scrollDirection: Axis.horizontal,
                  controller: x,
                  padding: const EdgeInsets.symmetric(horizontal: 10),
                  child: Column(
                    children: rows,
                  ),
                )),
          ],
        );
      }
    }
    return const Center(
        child: Text("No data available. Please press Preview button"));
  }
}
