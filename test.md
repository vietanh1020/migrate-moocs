# Chuân bị: Tạo site + tạo adminSite trước

B1: Sửa .env

B2: chạy lần lượt các script

# Tin tức

1. node news.js;

# Đơn vị

2. node manager_level_1.js
3. node manager_level_2.js
4. node manager_level_3.js

# Người dùng

5. node user.js

# Danh mục khóa hoc

6. node course_category.js

# Khóa học

7. node course.js

# Chương

8. node chapter.js

# Bài học

9. node lesson.js

IMPORT TABLE

docker cp "C:/Users/anh.voviet/Desktop/db/user_answers_4t.sql" 7b5299f40e8b:/tmp/user_answers_4t.sql

USE mobiedu_exam_4t;
SET SESSION sql_log_bin=0;

SOURCE /tmp/user_answers_4t.sql;

CREATE DATABASE mobiedu_exam_4t CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

docker cp C:\Users\anh.voviet\Desktop\backup\Course_new.sql df40b1975f56:/tmp/Course_new.sql

docker cp C:\Users\anh.voviet\Desktop\db\mschool_front_end.sql df40b1975f56:/tmp/mschool_front_end.sql
