import 'dart:developer';
import 'dart:html';

import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:flutter/material.dart';

import 'view/home_view.dart';

void main() {
  /* "contextpath" parameter will be injected by backend to dynamically retreive web address */
  var uri = Uri.dataFromString(window.location.href);
  Map<String, String> params = uri.queryParameters;
  var contextPath = params['contextpath'];

  log("contextpath retreive $contextPath");

  if (contextPath != null) {
    Utilities.webAPI = contextPath;
  }

  runApp(MaterialApp(
    routes: {HomeView.routeName: (context) => HomeView()},
    title: 'DM BigData',
    theme: ThemeData(
        fontFamily: "Roboto",
        primarySwatch: Colors.blueGrey,
        appBarTheme:
            const AppBarTheme(backgroundColor: Color.fromRGBO(22, 29, 37, 1)),
        elevatedButtonTheme: ElevatedButtonThemeData(
            style: ButtonStyle(
                backgroundColor: MaterialStateProperty.all(
                    const Color.fromRGBO(18, 61, 157, 1)))),
        textSelectionTheme:
            const TextSelectionThemeData(selectionColor: Colors.lightBlue),
        scrollbarTheme: ScrollbarThemeData(
            thumbVisibility: MaterialStateProperty.all<bool>(true))),
    home: HomeView(),
  ));
}
