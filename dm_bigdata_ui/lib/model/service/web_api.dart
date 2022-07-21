import 'dart:convert';
import 'dart:developer';

import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:http/http.dart' as http;

class WebAPIService {
  static WebAPIService? _instance;

  static const dataKey = "data";
  static const totalCountKey = "totalCount";
  static const fileStructureKey = "fileStructure";
  static const filePreviewKey = "filePreview";

  factory WebAPIService() {
    _instance ??= WebAPIService._internal();

    return WebAPIService._instance!;
  }

  WebAPIService._internal();

  /// request and get all files names loaded
  Future<List<String>> tablesNames() async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/tablesnames");

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }

  Future<Map<String, dynamic>> fileStructure(
      String filePath, bool excludeHeader) async {
    var url = Uri.parse(
        "${Utilities.webAPI}/webapi/filestructure?filePath=$filePath&excludeHeader=$excludeHeader");

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      var body = jsonDecode(response.body);

      var preview = List<List<String>>.from(
          (body[WebAPIService.filePreviewKey] ?? []).map((e) {
        var row = jsonDecode(e) as Map<String, dynamic>;

        return List<String>.generate(row.values.length, (i) {
          var cellule = row.values.elementAt(i);

          return cellule;
        });
      }).toList());

      // var structure = List<String>.from(jsonDecode(response.body));
      var structure = List<String>.from(body[WebAPIService.fileStructureKey]);

      Map<String, dynamic> result = {
        WebAPIService.filePreviewKey: preview,
        WebAPIService.fileStructureKey: structure
      };

      return result;
    } else {
      throw Exception(response.body);
    }
  }

  Future<List<String>> appColumns() async {
    var uri = "${Utilities.webAPI}/webapi/appcolumns";

    var url = Uri.parse(uri);

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }

  Future<List<String>> allJoins() async {
    var uri = "${Utilities.webAPI}/webapi/alljoins";

    var url = Uri.parse(uri);

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }

  Future<List<String>> appColumnsWithSource() async {
    var uri = "${Utilities.webAPI}/webapi/appcolumnswithsource";

    var url = Uri.parse(uri);

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }

  Future<void> updateAppColumn(
      String? oldColumnName, String newColumnName) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/updateappcolumn");

    var data = {"newColumnName": newColumnName};

    if (oldColumnName != null) {
      data.putIfAbsent("oldColumnName", () => oldColumnName);
    }

    var response = await http.post(url, body: data);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      log(response.body);
      return;
    } else {
      throw Exception(response.body);
    }
  }

  Future<void> deleteColumn(String columnName) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/deletecolumn");

    var data = {"columnName": columnName};

    var response = await http.post(url, body: data);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      log(response.body);
      return;
    } else {
      throw Exception(response.body);
    }
  }

  Future<void> updateJoin(String columnName, bool value) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/updatejoin");

    var data = {"columnName": columnName, "value": '$value'};

    var response = await http.post(url, body: data);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      log(response.body);
      return;
    } else {
      throw Exception(response.body);
    }
  }

  Future<List<String>> filesToImport() async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/filestoimport");

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }

  Future<void> importFile(String fileName, String tableName,
      Map<String, String?> fileStructure, bool excludeHeader) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/importfile");

    var data = {
      "filePath": fileName,
      "tableName": tableName,
      "fileStructure": jsonEncode(fileStructure),
      "excludeHeader": jsonEncode(excludeHeader)
    };

    var response = await http.post(url, body: data);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      log(response.body);
      return;
    } else {
      throw Exception(response.body);
    }
  }

  Future<void> dropTable(String? tableName) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/droptable");

    var data = {"tableName": tableName};

    var response = await http.post(url, body: data);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      log(response.body);
      return;
    } else {
      throw Exception(response.body);
    }
  }

  Future<Map<String, dynamic>> data(
      // String? tableName,
      String? filterExpr,
      int? offset,
      int? limit) async {
    var uri = "${Utilities.webAPI}/webapi/data?offset=$offset&limit=$limit";

    // if (tableName != null) {
    //   uri += "&tableName=$tableName";
    // }

    if (filterExpr != null) {
      uri += "&filter=$filterExpr";
    }

    var url = Uri.parse(uri);

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      var body = jsonDecode(response.body);

      var data =
          List<List<String>>.from((body[WebAPIService.dataKey] ?? []).map((e) {
        var row = jsonDecode(e) as Map<String, dynamic>;

        return List<String>.generate(row.values.length, (i) {
          var cellule = row.values.elementAt(i);

          return cellule;
        });
      }).toList());

      num totalCount = body[WebAPIService.totalCountKey];

      Map<String, dynamic> result = {
        WebAPIService.dataKey: data,
        WebAPIService.totalCountKey: totalCount
      };

      return result;
    } else {
      throw Exception(response.body);
    }
  }

  Future<List<List<String>>> tablesStatus() async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/tablesstatus");

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      var data = jsonDecode(response.body);

      var result = List<List<String>>.from((data ?? []).map((e) {
        return List<String>.from(e);
      }).toList());

      return result;
    } else {
      throw Exception(response.body);
    }
  }

  Future<List<String>> tablesStatusHeader() async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/tablesstatusheader");

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }
}
