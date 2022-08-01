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
  var dataMonitoringColumns = <String>[];
  PlutoGridStateManager? dataMonitoringTableManager;
  var dataMonitoringLoading = false;

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
    var plutoColumns = widget._utilities
        .generateColumnPlutoTable(widget.dataMonitoringColumns);

    /* rows are completed async */
    var plutoRows = widget._utilities
        .generateRowsPlutoTable(<List<String>>[], widget.dataMonitoringColumns);

    return SingleChildScrollView(
      child: Center(
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
              child: widget.dataMonitoringLoading
                  ? const CircularProgressIndicator()
                  : IconButton(
                      onPressed: () {
                        refreshData();
                      },
                      icon: const Icon(Icons.refresh)),
            ),
            widget.dataMonitoringColumns.isNotEmpty
                ? SizedBox(
                    height: Utilities.fieldHeight * 10,
                    child: PlutoGrid(
                      columns: plutoColumns,
                      rows: plutoRows,
                      onChanged: (PlutoGridOnChangedEvent event) {
                        log(event.toString());
                      },
                      onLoaded: (PlutoGridOnLoadedEvent event) {
                        // if (widget.dataMonitoringTableManager == null) {
                        widget.dataMonitoringTableManager = event.stateManager;
                        widget.dataMonitoringTableManager?.setShowLoading(true);

                        loadDataMonitoring();
                        // }
                      },
                      configuration: const PlutoGridConfiguration(
                          columnSize: PlutoGridColumnSizeConfig(
                              autoSizeMode: PlutoAutoSizeMode.scale)),
                    ))
                : const Text("..."),
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

  void loadDataMonitoringColumns() {
    setState(() {
      widget.dataMonitoringLoading = true;
    });

    widget._webAPIService.tablesStatusHeader().then((value) {
      widget.dataMonitoringColumns.clear();
      widget.dataMonitoringColumns.addAll(value);
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
      widget.dataMonitoringColumns.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataMonitoringLoading = false;
      });
    });
  }

  void loadDataMonitoring() async {
    widget._webAPIService.tablesStatus().then((value) async {
      if (value.isNotEmpty) {
        var plutoColumns = widget._utilities
            .generateColumnPlutoTable(widget.dataMonitoringColumns);

        var plutoRows = widget._utilities
            .generateRowsPlutoTable(value, widget.dataMonitoringColumns);

        PlutoGridStateManager.initializeRowsAsync(
          plutoColumns,
          plutoRows,
        ).then((value) {
          widget.dataMonitoringTableManager?.refRows.clear();
          widget.dataMonitoringTableManager?.refRows
              .addAll(FilteredList(initialList: value));

          widget.dataMonitoringTableManager?.setShowLoading(false);
        });
      } else {
        widget.dataMonitoringTableManager?.setShowLoading(false);
      }
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", stackTrace: stackTrace);
      widget.dataMonitoringTableManager?.setShowLoading(false);
    });
  }

  void refreshData() {
    loadDataMonitoringColumns();
  }
}
