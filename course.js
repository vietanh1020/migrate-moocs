import mysql from "mysql2/promise";
import { MongoClient, ObjectId } from "mongodb";
import dotenv from "dotenv";
import moment from "moment";
// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_COURSE,
    MONGO_URL,
    BATCH_SIZE,
    NEW_SITE_ID,
    OLD_SITE_ID,
    MONGO_DATABASE_COURSE,
    MONGO_DATABASE_USER,
    MYSQL_DATABASE_AUTH
} = process.env;


// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_COURSE,
    });
}

async function connectMySQLUser() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_AUTH,
    });
}


// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${OLD_SITE_ID} LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findCategory(records, db) {
    if (records.length === 0) return [];
    const ids = records.map(item => item.IdCategory);
    const rows = await db.collection("courseCatalog").find({ oldId: { $in: ids }, siteId: +NEW_SITE_ID, }).toArray();
    return rows;
}



async function CreateOrFindTeacher(teacherIds, mongoDbCourse, sqlConnectionUser) {
    if (!Array.isArray(teacherIds)) {
        teacherIds = [teacherIds];
    }

    const teacherObjectIds = [];

    for (const teacherId of teacherIds) {
        if (!teacherId) continue;




        let teacher = await mongoDbCourse.collection('teachers').findOne({ oldId: teacherId, siteId: +NEW_SITE_ID });

        if (!teacher) {


            const query = `SELECT * FROM Users WHERE Id=${teacherId}`;
            const [rows] = await sqlConnectionUser.execute(query);
            const user = rows[0];

            if (!user) {
                console.warn(`Teacher with ID ${teacherId} not found in SQL DB`);
                continue;
            }

            const newTeacher = {
                _id: new ObjectId(),
                avatar: user.AvatarUrl,
                fullName: user.FullName,
                email: user.Email,
                phone: user.Phone,
                personal: "",
                linkYoutube: "",
                linkFb: "",
                description: user.Description,
                siteId: +NEW_SITE_ID,
                oldId: teacherId,
                createdAt: moment().unix()
            };

            const result = await mongoDbCourse.collection('teachers').insertOne(newTeacher);
            teacher = newTeacher;
        }

        teacherObjectIds.push(teacher._id.toString());
    }

    return teacherObjectIds;
}



async function deleteOldUnit(db, level) {
    await db.collection(level).deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null }
    });
}

async function migrateTable(sqlConnection, mongoDbCourse, tableName, sqlConnectionUser) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);


    await deleteOldUnit(mongoDbCourse, 'course')
    await deleteOldUnit(mongoDbCourse, 'teachers')
    await deleteOldUnit(mongoDbCourse, 'chapter')
    await deleteOldUnit(mongoDbCourse, 'lesson')

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const category = await findCategory(rows, mongoDbCourse);


        console.log({ category });


        const mappedCourse = [];

        for (const row of rows) {
            const categoryItem = category.find(item => row.IdCategory == item.oldId);
            const cateId = categoryItem ? categoryItem._id.toString() : '';

            mappedCourse.push({
                name: row.Name,
                authorId: '', // authorId.toString()
                isHidden: 0,
                avatarURL: row.ThumbnailFileUrl,
                coverImageURL: row.CoverFileUrl,
                introVideoURL: "",
                catalogId: cateId,
                teacherId: "", // teacherId.toString()
                teacherIds: await CreateOrFindTeacher([...new Set([row.IDTeacher, row.IDCoTeacher].filter(Boolean))], mongoDbCourse, sqlConnectionUser),

                intro: row.WelcomeCourse,
                info: row.AboutCourse,
                benefit: row.Benefits,
                siteId: +NEW_SITE_ID,
                isCommented: true,
                totalRating: (row.Review || "").TotalReviews,
                averageStar: (row.Review || "").TotalStars,
                isSoftDeleted: row.IsDeleted,
                chapters: null,
                isRegister: row.IsOpenCourse == 1 ? 0 : 1,
                view: 10000,
                status: row.Status == 1 ? 1 : 0,
                createAt: moment(row.CreatedAt).unix(),
                updateAt: moment(row.ModifiedAt).unix(),
                backgroundCertificate: null,
                companyId: -1,
                createOn: moment().unix(),
                isCertification: true,
                numberLesson: +row.Price || 0,
                requiredScore: +row.SellingPrice || 0,
                oldId: row.Id
            });
        }

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) {
            break;
        }

        await mongoDbCourse.collection('course').insertMany(mappedCourse);

        offset += +BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();
const sqlConnectionUser = await connectMySQLUser();
// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDbCourse = mongoClient.db(MONGO_DATABASE_COURSE);

    try {
        await migrateTable(sqlConnection, mongoDbCourse, 'Courses', sqlConnectionUser);
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();
