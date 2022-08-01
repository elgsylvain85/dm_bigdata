import 'dart:async';
import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';
import 'package:pluto_grid/pluto_grid.dart';

class DataView extends StatefulWidget {
  String? filterExpr;
  final _webAPIService = WebAPIService();
  final _utilities = Utilities();

  var dataInitialized = false;
  var dataLoading = false;

  var dataColumns = <String>[];
  var offset = 0;
  var limit = Utilities.limitDataLoading;
  // num totalRows = 0;
  num rowsShowed = 0;
  PlutoGridStateManager? dataTableStateManager;

  var enableLoadMore = true;

  DataView({Key? key, this.filterExpr}) : super(key: key);

  @override
  State<StatefulWidget> createState() => _DataViewState();
}

class _DataViewState extends State<DataView> {
  @override
  void didChangeDependencies() {
    super.didChangeDependencies();

    /* check that data has been initialized in preview state to no need to reload again */
    if (!widget.dataInitialized) {
      refreshData();

      widget.dataInitialized = true;
    }
  }

  @override
  Widget build(BuildContext context) {
    if (widget.dataLoading) {
      return const Center(
        child: CircularProgressIndicator(),
      );
    } else {
      var plutoColumns =
          widget._utilities.generateColumnPlutoTable(widget.dataColumns);

      /* rows are completed async */
      var plutoRows = widget._utilities
          .generateRowsPlutoTable(<List<String>>[], widget.dataColumns);

      var tableHeight = MediaQuery.of(context).size.height * 0.75;

      return widget.dataColumns.isNotEmpty
          ? SingleChildScrollView(
              child: Column(
              crossAxisAlignment: CrossAxisAlignment.center,
              children: [
                /* Refresh button */
                Center(
                  child: IconButton(
                      onPressed: () {
                        refreshData();
                      },
                      icon: const Icon(Icons.refresh)),
                ),
                /* filter zone only if expression exist */
                widget.filterExpr != null && widget.filterExpr!.isNotEmpty
                    ? SingleChildScrollView(
                        scrollDirection: Axis.horizontal,
                        child: Row(
                          children: [
                            SelectableText(
                              "Filter : ${widget._utilities.cleanFilterExpr(widget.filterExpr)}",
                            ),
                            IconButton(
                                onPressed: () {
                                  clearFilter();
                                },
                                icon: const Icon(
                                  Icons.clear,
                                  color: Colors.red,
                                ))
                          ],
                        ),
                      )
                    : const SizedBox.shrink(),
                /* rows status */
                // SelectableText(
                //     "Rows : ${widget.rowsShowed} / ${widget.totalRows}"),
                SelectableText("${widget.rowsShowed} rows loaded"),
                ElevatedButton(
                  child: const Text(
                    "Export result",
                  ),
                  onPressed: () {
                    exportResult();
                  },
                ),
                SizedBox(
                  height: tableHeight < 450
                      ? Utilities.fieldHeight * 15
                      : tableHeight,
                  // width: 800,
                  child: PlutoGrid(
                    columns: plutoColumns,
                    rows: plutoRows,
                    onChanged: (PlutoGridOnChangedEvent event) {
                      log(event.toString());
                    },
                    onLoaded: (PlutoGridOnLoadedEvent event) {
                      // if (widget.dataTableStateManager == null) {
                      widget.dataTableStateManager = event.stateManager;
                      widget.dataTableStateManager?.setShowLoading(true);

                      loadData();
                      // }
                    },
                    configuration: const PlutoGridConfiguration(),
                    createFooter: (state) {
                      return SizedBox(
                        height: Utilities.fieldHeight,
                        child: Center(
                            child: TextButton.icon(
                                onPressed: () {
                                  if (widget.enableLoadMore) {
                                    loadMoreData();
                                  } else {
                                    log("limit excedeed");
                                  }
                                },
                                icon: const Icon(Icons.arrow_circle_down),
                                label: const Text("Load more"))),
                      );
                    },
                  ),
                )
              ],
            ))
          : Center(
              child: TextButton.icon(
                  onPressed: () {
                    refreshData();
                  },
                  icon: const Icon(Icons.refresh),
                  label: const Text(
                      "No data available. Please wait then refresh or use Search form")));
    }
  }

  Future<void> loadData() async {
    setState(() {
      widget.rowsShowed = 0;
      // widget.totalRows = 0;
    });

    widget._webAPIService
        .data(widget.filterExpr, widget.offset, widget.limit)
        .then((value) async {
      List<List<String>> data = value.containsKey(WebAPIService.dataKey)
          ? value[WebAPIService.dataKey]
          : [];

      if (data.isNotEmpty) {
        /* if data received is < limit means no longer has any additional data */

        setState(() {
          if (data.length < widget.limit) {
            widget.enableLoadMore = false;
          } else {
            widget.enableLoadMore = true;
          }
        });

        var plutoColumns =
            widget._utilities.generateColumnPlutoTable(widget.dataColumns);

        /* add only news rows to datatable : by remove lenght of rows already exist */
        var newRows = data.sublist(
            widget.dataTableStateManager?.refRows.length ??
                0); // temporally to fix offset and limit

        var plutoRows = widget._utilities
            .generateRowsPlutoTable(newRows, widget.dataColumns);

        PlutoGridStateManager.initializeRowsAsync(
          plutoColumns,
          plutoRows,
        ).then((value) {
          widget.dataTableStateManager?.refRows
              .addAll(FilteredList(initialList: value));

          widget.dataTableStateManager?.setShowLoading(false);

          setState(() {
            widget.rowsShowed =
                widget.dataTableStateManager?.refRows.length ?? 0;
          });
        });
      } else {
        widget.dataTableStateManager?.setShowLoading(false);

        setState(() {
          widget.rowsShowed = 0;
        });
      }
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", stackTrace: stackTrace);
      // widget.data.completeError(error, stackTrace);
      widget.dataTableStateManager?.setShowLoading(false);
    });
  }

  /// increase limit data loading then load data
  void loadMoreData() async {
    widget.dataTableStateManager?.setShowLoading(true);
    widget.limit += Utilities.limitDataLoading;
    await loadData();
  }

  /// restore default limit data loading
  void resetLimit() {
    widget.limit = Utilities.limitDataLoading;
  }

  void loadDataColumns() {
    setState(() {
      widget.dataLoading = true;
    });

    widget._webAPIService.appColumnsWithSource().then((value) {
      widget.dataColumns.clear();
      widget.dataColumns.addAll(value);
    }).catchError((error, stackTrace) {
      widget.dataColumns.clear();
    }).whenComplete(() {
      setState(() {
        widget.dataLoading = false;
      });
    });
  }

  void refreshData() {
    resetLimit();
    loadDataColumns();
    // loadData();
  }

  void clearFilter() {
    widget.filterExpr = null;
    refreshData();
  }

  Future<void> exportResult() async {
    widget._webAPIService
        .exportResult(widget.filterExpr, widget.offset, widget.limit)
        .then((_) {
      var snackBar = const SnackBar(
          content: Text("Operation initiated, please check Status form"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", stackTrace: stackTrace);

      var snackBar = SnackBar(content: Text("${error?.toString()}"));
      ScaffoldMessenger.of(context).showSnackBar(snackBar);
    });
  }
}
