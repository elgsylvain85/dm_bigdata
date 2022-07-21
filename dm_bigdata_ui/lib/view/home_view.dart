import 'dart:developer';

import 'package:dm_bigdata_ui/model/service/web_api.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:dm_bigdata_ui/view/data_view.dart';
import 'package:dm_bigdata_ui/view/filter_view.dart';
import 'package:dm_bigdata_ui/view/import_view.dart';
import 'package:dm_bigdata_ui/view/setting_view.dart';
import 'package:dm_bigdata_ui/view/status_view.dart';
import 'package:flutter/gestures.dart';
import 'package:flutter/material.dart';

class HomeView extends StatefulWidget {
  static const routeName = "homePage";
  static const initialIndexArg = "initialIndex";
  // static const fileNameArg = "fileName";
  static const filterExprArg = "filterExp";

  final _structureService = WebAPIService();
  final _utilities = Utilities();
  final _dropdownColumnFilter = GlobalKey<FormFieldState>();
  final _textFieldColumnFilter = GlobalKey<FormFieldState>();

  var dataInitialized = false;
  var columnsNames = <String>[];
  String? filterExpr;
  String? fileName;
  var initialIndex = 0;

  Widget? dataView;
  Widget? filterView;
  Widget? importView;
  Widget? statusView;
  Widget? settingView;

  // final String? fileName;

  HomeView({Key? key}) : super(key: key);

  @override
  State<StatefulWidget> createState() => _HomeViewState();
}

class _HomeViewState extends State<HomeView> {
  @override
  void didChangeDependencies() {
    super.didChangeDependencies();

    if (!widget.dataInitialized) {
      loadArgs();
      loadColumnsNames(); // call after load args

      widget.dataView = DataView(
          // fileName: widget.fileName,
          filterExpr: widget.filterExpr);
      widget.filterView = FilterView();
      widget.importView = ImportView();
      widget.statusView = StatusView();
      widget.settingView = SettingView();
      widget.dataInitialized = true;
    }
  }

  @override
  Widget build(BuildContext context) {
    /* prepares items of columns names */

    var columnsItems = <DropdownMenuItem<String?>>[];

    var item = const DropdownMenuItem<String?>(
      value: null,
      child: Text("All Columns"),
    );

    columnsItems.add(item);

    for (int i = 0; i < widget.columnsNames.length; i++) {
      var colName = widget.columnsNames[i];

      var item = DropdownMenuItem<String?>(
        value: colName,
        child: Text(colName),
      );

      columnsItems.add(item);
    }

    return DefaultTabController(
        length: 5,
        initialIndex: widget.initialIndex,
        child: Scaffold(
          appBar: AppBar(
            title: Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                const Padding(
                  padding: EdgeInsets.fromLTRB(15, 0, 45, 0),
                  child: Text("BigData",
                      style: TextStyle(fontWeight: FontWeight.bold)),
                ),
                SizedBox(
                    width: 150,
                    height: Utilities.fieldHeight,
                    child: DropdownButtonFormField<String?>(
                        key: widget._dropdownColumnFilter,
                        style: Utilities.itemStyle,
                        decoration: const InputDecoration(
                            filled: true,
                            fillColor: Utilities.fieldFillColor,
                            border: OutlineInputBorder(),
                            contentPadding: EdgeInsets.symmetric(horizontal: 10)
                            // hintText: "All Column",
                            // suffixIcon: Icon(Icons.search),
                            ),
                        items: columnsItems,
                        onChanged: (value) {})),
                SizedBox(
                    width: Utilities.fieldWidth,
                    height: Utilities.fieldHeight,
                    child: TextFormField(
                        key: widget._textFieldColumnFilter,
                        style: Utilities.itemStyle,
                        decoration: const InputDecoration(
                          filled: true,
                          fillColor: Utilities.fieldFillColor,
                          border: OutlineInputBorder(),
                          contentPadding: EdgeInsets.symmetric(horizontal: 10),
                          hintText: "Search",
                          suffixIcon: Icon(Icons.search),
                        ),
                        onFieldSubmitted: (value) {
                          /* Filter applying */

                          String? sourcesExpr;
                          String? columnsExpr;

                          /* if filename is not null then pass that as first filter argument on special column "Sources"*/

                          // String? fileName = widget.fileName;

                          if (widget.fileName != null) {
                            var expr = widget._utilities.filterExpression(
                                Utilities.sourceColumn, widget.fileName!);

                            if (sourcesExpr != null &&
                                sourcesExpr.trim().isNotEmpty) {
                              //use OR SQL expression
                              sourcesExpr = "$sourcesExpr} or $expr";
                            } else {
                              sourcesExpr = expr;
                            }
                          }

                          /* apply column filter only if text field is not empty */

                          if (value.trim().isNotEmpty) {
                            var colName = widget
                                ._dropdownColumnFilter.currentState?.value;

                            if (colName != null) {
                              /* apply filter on one column */

                              columnsExpr = widget._utilities
                                  .filterExpression(colName, value);
                            } else {
                              /* apply filter on all columns */
                              columnsExpr = null;
                              for (int i = 0;
                                  i < widget.columnsNames.length;
                                  i++) {
                                String colName = widget.columnsNames[i];

                                var expr = widget._utilities
                                    .filterExpression(colName, value);

                                if (columnsExpr != null &&
                                    columnsExpr.trim().isNotEmpty) {
                                  //use OR SQL expression
                                  columnsExpr = "$columnsExpr or $expr";
                                } else {
                                  columnsExpr = expr;
                                }
                              }
                            }
                          }

                          if (sourcesExpr != null && columnsExpr != null) {
                            widget.filterExpr =
                                "($sourcesExpr) and ($columnsExpr)";
                          } else if (sourcesExpr != null) {
                            widget.filterExpr = sourcesExpr;
                          } else if (columnsExpr != null) {
                            widget.filterExpr = columnsExpr;
                          }

                          //  else {
                          //   /* disable filter if text field is empty */

                          //   widget.filterExpr = null;
                          // }

                          // setState(() {});
                          Navigator.pushNamedAndRemoveUntil(
                              context,
                              HomeView.routeName,
                              (Route<dynamic> route) => false,
                              arguments: {
                                // HomeView.fileNameArg: widget.fileName,
                                HomeView.filterExprArg: widget.filterExpr
                              });
                        })),
                const Expanded(
                    child: TabBar(tabs: [
                  Tab(
                    icon: Icon(
                      Icons.home,
                    ),
                  ),
                  Tab(text: "Search form"),
                  Tab(text: "Import"),
                  Tab(text: "Status"),
                  Tab(text: "Settings"),
                ]))
              ],
            ),
            actions: [
              IconButton(onPressed: () {}, icon: const Icon(Icons.logout))
            ],
          ),
          body: TabBarView(
              physics: const NeverScrollableScrollPhysics(),
              children: [
                widget.dataView!,
                widget.filterView!,
                widget.importView!,
                widget.statusView!,
                widget.settingView!
              ]),
        ));
  }

  void loadColumnsNames() {
    widget._structureService.appColumns().then((value) {
      widget.columnsNames.clear();

      setState(() {
        widget.columnsNames.addAll(value);
      });
    }).catchError((error, stackTrace) {
      log("${error?.toString()}", error: error, stackTrace: stackTrace);
    });
  }

  void loadArgs() {
    /* check and get arguments if present */

    var args = ModalRoute.of(context)?.settings.arguments;

    if (args != null) {
      var argsAsMap = args as Map<String, String?>;

      /* get initial tab */

      if (argsAsMap.containsKey(HomeView.initialIndexArg)) {
        widget.initialIndex =
            int.tryParse(argsAsMap[HomeView.initialIndexArg]!) ?? 0;
      }

      // // /* get fileName: used in dataPage */

      // if (argsAsMap.containsKey(HomeView.fileNameArg)) {
      //   widget.fileName = argsAsMap[HomeView.fileNameArg];
      // } else {
      //   widget.fileName = null;
      // }

      /* get filterExpr: used in dataPage */

      if (argsAsMap.containsKey(HomeView.filterExprArg)) {
        widget.filterExpr = argsAsMap[HomeView.filterExprArg];
      } else {
        widget.filterExpr = null;
      }
    }
  }
}
