import 'dart:convert';
import 'dart:developer';

import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:file_picker/src/platform_file.dart';
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
  Future<List<String>> tablesImported() async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/tablesimported");

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      return List<String>.from(jsonDecode(response.body));
    } else {
      throw Exception(response.body);
    }
  }

  Future<Map<String, dynamic>> loadPreviewFile(
      String filePath, bool excludeHeader, String? delimiter) async {
    var uri =
        "${Utilities.webAPI}/webapi/filestructure?filePath=$filePath&excludeHeader=$excludeHeader";

    if (delimiter != null) {
      uri += "&delimiter=$delimiter";
    }

    var url = Uri.parse(uri);

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      var body = jsonDecode(response.body);

      /* header */

      var structure = List<String>.from(body[WebAPIService.fileStructureKey]);

      /* data */

      var preview = List<List<String>>.from(
          (body[WebAPIService.filePreviewKey] ?? []).map((e) {
        var result = <String>[];

        var row = jsonDecode(e) as Map<String, dynamic>;

        for (var c in structure) {
          if (row.containsKey(c)) {
            result.add(row[c]);
          } else {
            result.add("");
          }
        }

        // return List<String>.generate(row.values.length, (i) {
        //   var cellule = row.values.elementAt(i);

        //   return cellule;
        // });
        return result;
      }).toList());

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

  Future<void> importFile(
      String? filePath,
      String? source,
      Map<String, String?> columnsMapping,
      bool excludeHeader,
      String? delimiter) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/importfile");

    var data = {
      "filePath": filePath,
      "source": source,
      "columnsMapping": jsonEncode(columnsMapping),
      "excludeHeader": jsonEncode(excludeHeader)
    };

    if (delimiter != null) {
      data.putIfAbsent("delimiter", () => delimiter);
    }

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

    var data = {};

    if (tableName != null) {
      data.putIfAbsent("tableName", () => tableName);
    }

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

      /* header */

      var structure = List<String>.from(body[WebAPIService.fileStructureKey]);

      /* data */

      var data =
          List<List<String>>.from((body[WebAPIService.dataKey] ?? []).map((e) {
        // var row = jsonDecode(e) as Map<String, dynamic>;

        // return List<String>.generate(row.values.length, (i) {
        //   var cellule = row.values.elementAt(i);

        //   return cellule;
        // });

        var result = <String>[];

        var row = jsonDecode(e) as Map<String, dynamic>;

        for (var c in structure) {
          if (row.containsKey(c)) {
            result.add(row[c]);
          } else {
            result.add("");
          }
        }

        return result;
      }).toList());

      // num totalCount = body[WebAPIService.totalCountKey];

      Map<String, dynamic> result = {
        WebAPIService.dataKey: data,
        // WebAPIService.totalCountKey: totalCount
      };

      return result;
    } else {
      throw Exception(response.body);
    }
  }

  Future<void> exportResult(String? filterExpr, int? offset, int? limit) async {
    var uri =
        "${Utilities.webAPI}/webapi/exportresult?offset=$offset&limit=$limit";

    if (filterExpr != null) {
      uri += "&filter=$filterExpr";
    }

    var url = Uri.parse(uri);

    var response = await http.get(url);

    if (response.statusCode >= 200 && response.statusCode <= 299) {
      log(response.body);
      return;
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

  Future<String> uploadFile(PlatformFile file) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/uploadfile");

    var fileReadStream = file.readStream;

    if (fileReadStream != null) {
      var stream = http.ByteStream(file.readStream!);

      var multipart =
          http.MultipartFile("media", stream, file.size, filename: file.name);

      var request = http.MultipartRequest("POST", url)..files.add(multipart);

      var response = await request.send();

      if (response.statusCode >= 200 && response.statusCode <= 299) {
        var body = await response.stream.toBytes();
        return String.fromCharCodes(body);
      } else {
        var body = await response.stream.toBytes();
        throw Exception(String.fromCharCodes(body));
      }
    } else {
      throw Exception("Cannot read file from stream");
    }
  }
}
