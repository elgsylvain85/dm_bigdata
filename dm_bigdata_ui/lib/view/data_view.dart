import 'dart:async';
import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';
import 'package:pluto_grid/pluto_grid.dart';

class DataView extends StatefulWidget {
  // final String? fileName;
  String? filterExpr;
  final _webAPIService = WebAPIService();
  final _utilities = Utilities();

  var dataInitialized = false;
  var dataLoading = false;

  // var data = Completer<List<List<String>>>();
  var dataColumns = Completer<List<String>>();
  var offset = 0;
  var limit = Utilities.limitDataLoading;
  num totalRows = 0;
  num rowsShowed = 0;
  late PlutoGridStateManager dataTableStateManager;

  var enableLoadMore = true;

  DataView(
      {Key? key,
      // this.fileName,
      this.filterExpr})
      : super(key: key);

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
    return Center(
      child: SingleChildScrollView(
          child: Column(
        mainAxisAlignment: MainAxisAlignment.spaceAround,
        crossAxisAlignment: CrossAxisAlignment.center,
        children: [
          /* data table */
          FutureBuilder<List<String>>(
              future: widget.dataColumns.future,
              builder: (context, snapshot) {
                if (snapshot.connectionState == ConnectionState.done &&
                    snapshot.hasData) {
                  var columns = snapshot.requireData;

                  var plutoColumns =
                      widget._utilities.generateColumnPlutoTable(columns);

                  /* rows are completed async */
                  var plutoRows = widget._utilities
                      .generateRowsPlutoTable(<List<String>>[], columns);

                  var tableHeight = MediaQuery.of(context).size.height * 0.80;

                  return Column(
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
                          ? Row(
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
                            )
                          : const SizedBox.shrink(),
                      /* rows status */
                      SelectableText(
                          "Rows : ${widget.rowsShowed} / ${widget.totalRows}"),
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
                            widget.dataTableStateManager = event.stateManager;
                            widget.dataTableStateManager.setShowLoading(true);

                            loadData();
                          },
                          configuration: const PlutoGridConfiguration(),
                          createFooter: (state) {
                            return SizedBox(
                              height: Utilities.fieldHeight,
                              child: Center(
                                  child: TextButton.icon(
                                      onPressed: widget.enableLoadMore
                                          ? () {
                                              if (widget.enableLoadMore) {
                                                loadMoreData();
                                              } else {
                                                log("limit excedeed");
                                              }
                                            }
                                          : null,
                                      icon: const Icon(Icons.arrow_circle_down),
                                      label: const Text("Load more"))),
                            );
                          },
                        ),
                      )
                    ],
                  );
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

                  return Center(
                      child: TextButton.icon(
                          onPressed: () {
                            refreshData();
                          },
                          icon: const Icon(Icons.refresh),
                          label: const Text(
                              "No data available. Please use Search form or wait then refresh")));
                }
              })
        ],
      )),
    );
  }

  Future<void> loadData() async {
    widget._webAPIService
        .data(
            // widget.fileName,
            widget.filterExpr,
            widget.offset,
            widget.limit)
        .then((value) async {
      List<List<String>> data = value.containsKey(WebAPIService.dataKey)
          ? value[WebAPIService.dataKey]
          : [];
      setState(() {
        widget.totalRows = value.containsKey(WebAPIService.totalCountKey)
            ? value[WebAPIService.totalCountKey]
            : 0;
      });

      if (data.isNotEmpty) {
        if (widget.dataColumns.isCompleted) {
          /* if data received is < limit means no longer has any additional data */

          if (data.length < widget.limit) {
            widget.enableLoadMore = false;
          } else {
            widget.enableLoadMore = true;
          }

          var columns = await widget.dataColumns.future;

          var plutoColumns =
              widget._utilities.generateColumnPlutoTable(columns);

          /* add only news rows to datatable : by remove lenght of rows already exist */
          var newRows = data.sublist(widget.dataTableStateManager.refRows
              .length); // temporally to fix offset and limit

          var plutoRows =
              widget._utilities.generateRowsPlutoTable(newRows, columns);

          PlutoGridStateManager.initializeRowsAsync(
            plutoColumns,
            plutoRows,
          ).then((value) {
            widget.dataTableStateManager.refRows
                .addAll(FilteredList(initialList: value));

            widget.dataTableStateManager.setShowLoading(false);

            setState(() {
              widget.rowsShowed = widget.dataTableStateManager.refRows.length;
            });
          });
        }
      } else {
        widget.dataTableStateManager.setShowLoading(false);

        setState(() {
          widget.rowsShowed = 0;
        });
      }
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", stackTrace: stackTrace);
      // widget.data.completeError(error, stackTrace);
      widget.dataTableStateManager.setShowLoading(false);
    });
  }

  /// increase limit data loading then load data
  void loadMoreData() async {
    widget.dataTableStateManager.setShowLoading(true);
    widget.limit += Utilities.limitDataLoading;
    await loadData();
  }

  /// restore default limit data loading
  void resetLimit() {
    widget.limit = Utilities.limitDataLoading;
  }

  void loadDataColumns() {
    setState(() {
      widget.dataColumns = Completer();
    });

    widget._webAPIService.appColumnsWithSource().then((value) {
      widget.dataColumns.complete(value);
    }).catchError((error, stackTrace) {
      widget.dataColumns.completeError(error, stackTrace);
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
}
