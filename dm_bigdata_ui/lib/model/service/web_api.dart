import 'dart:async';
import 'dart:convert';
import 'dart:developer';
import 'dart:html';
// import 'dart:io';

import 'package:dm_bigdata_ui/utility/local_uploader.dart';
import 'package:dm_bigdata_ui/utility/utilities.dart';
import 'package:http/http.dart' as http;

typedef void OnUploadProgressCallback(int sentBytes, int totalBytes);

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

  Future<void> applyJoinSources(List<String?> sources) async {
    var url = Uri.parse("${Utilities.webAPI}/webapi/applyjoinsources");

    var data = {
      "sources": jsonEncode(sources),
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

//   Future<String> uploadFile(PlatformFile file,
//       {void Function(double)? onProgress}) async {
//     var uri = "${Utilities.webAPI}/webapi/uploadfile";
//     // var url = Uri.parse(uri);

//     var fileReadStream = file.readStream;

//     if (fileReadStream != null) {
//       //   var multipart = http.MultipartFile("media", fileReadStream, file.size,
//       //       filename: file.name);

//       //   var requestMultipart =
//       //       LocalMultipartRequest("POST", url, onProgress: onProgress);

//       //   requestMultipart.files.add(multipart);

//       //   var httpResponse = await requestMultipart.send();
//       //   var body = await httpResponse.stream.toBytes();
//       //   var data = String.fromCharCodes(body);

//       //   if (httpResponse.statusCode ~/ 100 == 2) {
//       //     return data;
//       //   } else {
//       //     throw Exception(data);
//       //   }

//       LocalUploader chunkedUploader =
//           LocalUploader(Dio(BaseOptions(baseUrl: uri)));
//       // try {
//       Response? response = await chunkedUploader.upload(
//           uri: uri,
//           fileKey: "media",
//           file: fileReadStream.asBroadcastStream(),
//           fileName: file.name,
//           fileSize: file.size,
//           // maxChunkSize: 100000000,
//           maxChunkSize: 500000,
//           onUploadProgress: onProgress);

//       if (response != null && response.statusCode! ~/ 100 == 2) {
//         return response.data;
//       } else {
//         throw Exception(data);
//       }
//       // } on DioError catch (e) {
//       //   print(e);
//       // }
//     } else {
//       throw Exception("Cannot read file from stream");
//     }
//   }
// }

// class LocalMultipartRequest extends http.MultipartRequest {
//   /// Creates a new [LocalMultipartRequest].
//   LocalMultipartRequest(
//     String method,
//     Uri url, {
//     this.onProgress,
//   }) : super(method, url);

//   final void Function(int bytes, int totalBytes)? onProgress;

//   /// Freezes all mutable fields and returns a single-subscription [ByteStream]
//   /// that will emit the request body.
//   @override
//   http.ByteStream finalize() {
//     final byteStream = super.finalize();
//     if (onProgress == null) return byteStream;

//     final total = contentLength;
//     int bytes = 0;

//     final t = StreamTransformer.fromHandlers(
//       handleData: (List<int> data, EventSink<List<int>> sink) {
//         bytes += data.length;

//         if (onProgress != null) {
//           onProgress!(bytes, total);
//         }

//         if (total >= bytes) {
//           sink.add(data);
//         }
//       },
//     );
//     final stream = byteStream.transform(t);
//     return http.ByteStream(stream);
//   }
}
