StringBuilder sb = new StringBuilder();
for (int i = 0; i < data.length; i++) {
    sb.append(data[i]);
    if (i < data.length - 1) {
        sb.append(","); // Thêm dấu phẩy giữa các phần tử
    }
}
resultValue.set(sb.toString());
