package com.superior.datatunnel.plugin.http.feishu;

import com.lark.oapi.Client;
import com.lark.oapi.service.sheets.v3.model.GetSpreadsheetSheetReq;
import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class FeiShuDataTunnelSource implements DataTunnelSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeiShuDataTunnelSource.class);

    private void validateOptions(DataTunnelContext context) {}

    public Dataset<Row> read(DataTunnelContext context) throws IOException, URISyntaxException {
        FeiShuDataTunnelSourceOption sourceOption = (FeiShuDataTunnelSourceOption) context.getSourceOption();

        // 2. 从飞书 API 获取原始二维数组数据
        Object[][] rawData = fetchFeishuSheetData(sourceOption);

        // 3. 解析表头 (假设第一行为表头 Header)
        Object[] headerRow = rawData[0];
        List<StructField> fields = new ArrayList<>();
        for (Object header : headerRow) {
            // 默认将所有列作为 StringType 处理，后续可在 Spark 中自行 cast
            fields.add(DataTypes.createStructField(String.valueOf(header), DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // 4. 解析数据行 (从第二行开始)
        List<Row> rowList = new ArrayList<>();
        int columnCount = headerRow.length;

        for (int i = 1; i < rawData.length; i++) {
            Object[] rawRow = rawData[i];
            String[] rowValues = new String[columnCount];

            // 填充数据，防止飞书返回的行长度不一致或缺失
            for (int j = 0; j < columnCount; j++) {
                if (j < rawRow.length && rawRow[j] != null) {
                    rowValues[j] = String.valueOf(rawRow[j]);
                } else {
                    rowValues[j] = ""; // 缺失值补空字符串
                }
            }
            rowList.add(RowFactory.create((Object[]) rowValues));
        }

        return context.getSparkSession().createDataFrame(rowList, schema);
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return FeiShuDataTunnelSourceOption.class;
    }

    /**
     * 辅助方法：通过飞书 SDK 获取表格数据
     */
    private Object[][] fetchFeishuSheetData(FeiShuDataTunnelSourceOption sourceOption) {
        Client client = Client.newBuilder(sourceOption.getAppId(), sourceOption.getAppSecret())
                .build();

        GetSpreadsheetSheetReq sheetReq = GetSpreadsheetSheetReq.newBuilder()
                .spreadsheetToken(sourceOption.getSpreadsheetToken())
                .sheetId(sourceOption.getSheetId())
                .build();

        // GetSpreadsheetSheetResp sheetResp = client.sheets().spreadsheetSheet().get(sheetReq);

        return null;
    }
}
