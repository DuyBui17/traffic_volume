// Kiểm tra nếu giá trị hợp lệ và thêm vào bản đồ
if (temp != -1) {  // Kiểm tra nếu nhiệt độ hợp lệ
    trafficData.computeIfAbsent("temp", k -> new ArrayList<>()).add(temp);
}

if (rain != -1) {  // Kiểm tra nếu lượng mưa hợp lệ
    trafficData.computeIfAbsent("rain", k -> new ArrayList<>()).add(rain);
}

if (snow != -1) {  // Kiểm tra nếu lượng tuyết hợp lệ
    trafficData.computeIfAbsent("snow", k -> new ArrayList<>()).add(snow);
}

if (clouds != -1) {  // Kiểm tra nếu tỷ lệ mây hợp lệ
    trafficData.computeIfAbsent("clouds", k -> new ArrayList<>()).add(clouds);
}

if (trafficVolume != -1) {  // Kiểm tra nếu lưu lượng giao thông hợp lệ
    trafficData.computeIfAbsent("trafficVolume", k -> new ArrayList<>()).add(trafficVolume);
}
