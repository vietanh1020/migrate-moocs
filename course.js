import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";
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
    MONGO_DATABASE_USER
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
    const rows = await db.collection("courseCatalog").find({ oldId: { $in: ids } }).toArray();
    return rows;
}

async function findUsers(records, mongoDbUser) {
    if (records.length === 0) return [];

    const userIds = [];

    records.forEach(element => {
        if (element?.IDTeacher) userIds.push(element?.IDTeacher)
        if (element?.CreatedBy) userIds.push(element?.CreatedBy)
    });

    console.log({ userIds });

    const rows = await mongoDbUser.collection("user").find({ mobieduUserId: { $in: userIds } }).toArray();

    console.log({ rows });
    return rows;
}

async function migrateTable(sqlConnection, mongoDbCourse, tableName, mongoDbUser) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const category = await findCategory(rows, mongoDbCourse);
        const users = await findUsers(rows, mongoDbUser);

        const mappedCourse = rows.map((row) => {

            const authorId = users.find(item => row.CreatedBy == item.mobieduUserId)?._id || '';
            const teacherId = users.find(item => row.IDTeacher == item.mobieduUserId)?._id || '';
            const cateId = category.find(item => row.IdCategory == item.oldId)?._id || '';

            return {
                name: row.Name,
                authorId: '',  //authorId.toString()
                isHidden: 0,
                avatarURL: row.ThumbnailFileUrl,
                coverImageURL: row.CoverFileUrl,
                introVideoURL: "",
                catalogId: cateId.toString(),
                teacherId: "",//teacherId.toString()
                teacherIds: [""],

                intro: row.WelcomeCourse,
                info: row.AboutCourse,
                benefit: row.Benefits,
                siteId: +NEW_SITE_ID,
                isCommented: true,
                totalRating: (row.Review || "")?.TotalReviews,
                averageStar: (row.Review || "")?.TotalStars,
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
                // statusMarkScore: null,
                oldId: row.Id
            }
        });

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

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDbCourse = mongoClient.db(MONGO_DATABASE_COURSE);
    const mongoDbUser = mongoClient.db(MONGO_DATABASE_USER);


    try {
        await migrateTable(sqlConnection, mongoDbCourse, 'Courses', mongoDbUser);
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();
