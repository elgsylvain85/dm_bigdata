import 'dart:async';
import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';
import 'package:pluto_grid/pluto_grid.dart';

class StatusView extends StatefulWidget {
  final _webAPIService = WebAPIService();
  final _utilities = Utilities();

  var dataInitialized = false;
  var filesStatusColumns = Completer<List<String>>();
  late PlutoGridStateManager filesStatusTableManager;

  StatusView({Key? key}) : super(key: key);

  @override
  State<StatefulWidget> createState() => _StatusViewState();
}

class _StatusViewState extends State<StatusView> {
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
    return Center(
      child: SingleChildScrollView(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.spaceAround,
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            /* File Importation Status */
            const Text(
              "Data Monitoring",
              style: Utilities.titleStyle,
            ),
            Center(
              child: IconButton(
                  onPressed: () {
                    refreshData();
                  },
                  icon: const Icon(Icons.refresh)),
            ),
            FutureBuilder<List<String>>(
                future: widget.filesStatusColumns.future,
                builder: (context, snapshot) {
                  if (snapshot.connectionState == ConnectionState.done &&
                      snapshot.hasData) {
                    var columns = snapshot.requireData;

                    var plutoColumns =
                        widget._utilities.generateColumnPlutoTable(columns);

                    /* rows are completed async */
                    var plutoRows = widget._utilities
                        .generateRowsPlutoTable(<List<String>>[], columns);

                    return SizedBox(
                        height: Utilities.fieldHeight * 10,
                        child: PlutoGrid(
                          columns: plutoColumns,
                          rows: plutoRows,
                          onChanged: (PlutoGridOnChangedEvent event) {
                            log(event.toString());
                          },
                          onLoaded: (PlutoGridOnLoadedEvent event) {
                            widget.filesStatusTableManager = event.stateManager;
                            widget.filesStatusTableManager.setShowLoading(true);

                            loadFilesStatusData();
                          },
                          configuration: const PlutoGridConfiguration(
                              columnSize: PlutoGridColumnSizeConfig(
                                  autoSizeMode: PlutoAutoSizeMode.scale)),
                        ));
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
                      "An error has occurred",
                      style: Utilities.itemStyle,
                    ));
                  }
                }),
            /* server monitoring status */
            const Text(
              "Server Health Monitoring",
              style: Utilities.titleStyle,
            ),
          ],
        ),
      ),
    );
  }

  void loadFilesStatusColumns() {
    setState(() {
      widget.filesStatusColumns = Completer<List<String>>();
    });

    widget._webAPIService.tablesStatusHeader().then((value) {
      widget.filesStatusColumns.complete(value);
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.filesStatusColumns.completeError(error, stackTrace);
    });
  }

  void loadFilesStatusData() async {
    widget._webAPIService.tablesStatus().then((value) async {
      if (value.isNotEmpty) {
        if (widget.filesStatusColumns.isCompleted) {
          var columns = await widget.filesStatusColumns.future;

          var plutoColumns =
              widget._utilities.generateColumnPlutoTable(columns);

          var plutoRows =
              widget._utilities.generateRowsPlutoTable(value, columns);

          PlutoGridStateManager.initializeRowsAsync(
            plutoColumns,
            plutoRows,
          ).then((value) {
            widget.filesStatusTableManager.refRows.clear();
            widget.filesStatusTableManager.refRows
                .addAll(FilteredList(initialList: value));

            widget.filesStatusTableManager.setShowLoading(false);
          });
        }
      } else {
        widget.filesStatusTableManager.setShowLoading(false);
      }
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", stackTrace: stackTrace);
      widget.filesStatusTableManager.setShowLoading(false);
    });
  }

  void refreshData() {
    loadFilesStatusColumns();
  }
}
