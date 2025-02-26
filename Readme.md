# Chuân bị: Tạo site + tạo adminSite trước

B1: Sửa .env

B2: chạy lần lượt các script

# Đơn vị

2. node manager_level_1.js
3. node manager_level_2.js
4. node manager_level_3.js

# Người dùng

5. node user.js

6. node user_group.js

# Danh mục khóa hoc

6. node course_category.js

# Khóa học

7. node course.js

# Chương

8. node chapter.js

# Bài học

9. node lesson.js

# Tin tức

10. node news.js;

# SCRIPT docker

docker cp C:\Users\anh.voviet\Desktop\backup\Course_Categories.sql df40b1975f56:/Course_Categories.sql

docker cp C:\Users\anh.voviet\Desktop\backup\users-4t.sql df40b1975f56:/tmp/users-4t.sql
docker cp C:\Users\anh.voviet\Desktop\backup\questions_4t.sql df40b1975f56:/tmp/questions_4t.sql
docker cp C:\Users\anh.voviet\Desktop\backup\rooms_4t.sql df40b1975f56:/tmp/rooms_4t.sql
docker cp C:\Users\anh.voviet\Desktop\backup\exams_4t.sql df40b1975f56:/tmp/exams_4t.sql

USE mobiedu_exam_4t;
SET SESSION sql_log_bin=0;

SOURCE /tmp/users-4t.sql;
SOURCE /tmp/questions_4t.sql;
SOURCE /tmp/rooms_4t.sql;
SOURCE /tmp/exams_4t.sql;

CREATE DATABASE mobiedu_exam_4t CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

docker cp C:\Users\anh.voviet\Desktop\backup\Course_new.sql df40b1975f56:/tmp/Course_new.sql

docker cp C:\Users\anh.voviet\Desktop\db\mschool_front_end.sql df40b1975f56:/tmp/mschool_front_end.sql
