import mysql, { raw } from "mysql2/promise";
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
    MONGO_DATABASE_COURSE,
    MONGO_URL,
    BATCH_SIZE,
    NEW_SITE_ID,
    OLD_SITE_ID,
    MYSQL_DATABASE_4T,
    MONGO_DATABASE_USER,
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

async function deleteOldData(db, table) {
    await db.collection(table).deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null }  // Ensures oldId is not null
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IdSite = ${parseInt(OLD_SITE_ID)} 
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findListUser(records, dbUser) {
    if (records.length === 0) return [];
    const ids = [...new Set(records.map(item => item.IdUser))];

    const rows = await dbUser
        .collection("user")
        .find({ mobieduUserId: { $in: ids }, siteId: +NEW_SITE_ID })
        .toArray();
    return rows;
}

async function findCourses(mongoDbCourse) {
    const courses = await mongoDbCourse
        .collection("course")
        .find({ siteId: +NEW_SITE_ID })
        .toArray(); // Chuyển cursor thành mảng
    return courses;
}

async function findStudentCourse(userId, courseId, mongoDbCourse) {
    const studentCou = await mongoDbCourse.collection("studentCourse").findOne({ userId, courseId, siteId: +NEW_SITE_ID });
    return studentCou
}


async function getCertDefault(mongoDbCourse) {
    const certDefault = await mongoDbCourse.collection("mCertificate").findOne({ siteId: +NEW_SITE_ID });
    return certDefault;
}


async function migrateTable(sqlConnection, mongoDbExam, mongoDbCourse, mongoDbUser, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    await deleteOldData(mongoDbCourse, "studentCertificate")

    const listCourse = await findCourses(mongoDbCourse)

    const certConf = await getCertDefault(mongoDbCourse)

    let offset = 0;
    // tạo lớp học 

    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);
        const listUser = await findListUser(rows, mongoDbUser)

        const mappedCertificate = []

        for (const row of rows) {
            const userInMongo = listUser.find(item => item.mobieduUserId == row.IdUser)
            const course = listCourse.find(item => item.oldId == row.IdCourse)

            if (!userInMongo || !course) continue

            const userId = userInMongo?._id.toString()
            const courseId = course?._id.toString()

            const studentCourse = await findStudentCourse(userId, courseId, mongoDbCourse)

            if (!studentCourse) continue

            const newStudentAnswer = {
                userId,
                courseId,
                studentCourseId: studentCourse._id.toString(),
                classId: studentCourse.classId,
                certificateInfo: {
                    certificateName: course.name,
                    fullName: userInMongo.fullname,
                    position: "",
                    area: "Hà Nội"
                },
                courseName: course.name,
                className: course.name,
                mcertificateId: certConf._id.toString(),
                managerLevelId: null,
                managerLevelName: null,
                siteId: +NEW_SITE_ID,
                oldId: row.Id,
                createdAt: new Date(row.CreatedAt),
                certType: 1,
                certificateUrl: certConf.url
            }

            mappedCertificate.push(newStudentAnswer);
        }
        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;


        await mongoDbCourse.collection('studentCertificate').insertMany(mappedCertificate);

        offset += +BATCH_SIZE;
    }

}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDbExam = mongoClient.db('db_moocs_exam');
    const mongoDbCourse = mongoClient.db(MONGO_DATABASE_COURSE);
    const mongoDbUser = mongoClient.db(MONGO_DATABASE_USER)

    try {
        await migrateTable(sqlConnection, mongoDbExam, mongoDbCourse, mongoDbUser, 'CourseCertificate');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();