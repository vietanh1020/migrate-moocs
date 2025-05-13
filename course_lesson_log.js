import dotenv from "dotenv";
import { MongoClient, ObjectId } from "mongodb";
import mysql from "mysql2/promise";

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

function createStudentLesson(row, lesson, userId, studentCourse) {
    const lessonProgress = {
        _id: new ObjectId(),
        createdAt: new Date(row.CreatedDate),
        updatedAt: new Date(row.CreatedDate),
        classId: studentCourse.classId,
        lessonId: lesson._id.toString(),
        studentCourseId: studentCourse._id.toString(),
        courseId: lesson.courseId,
        chapterId: lesson.chapterId,
        order: lesson.order,
        userId: userId,
        status: row.IsComplete ? 1 : 0,
        completedPercent: row.IsComplete ? 100 : 0,
        isLocked: false,
        noteContent: null,
        currentTime: 0,
        questions: null,
        completedTime: 0,
        roomTestId: lesson.roomTestId,
        isPassRoomTest: false,
        oldId: row.Id,
        siteId: +NEW_SITE_ID
    };

    return lessonProgress
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

async function findListLesson(dbCourse) {
    const rows = await dbCourse
        .collection("lesson")
        .find({ siteId: +NEW_SITE_ID })
        .toArray();
    return rows;
}

async function findStudentCourse(rawData, dbCourse) {
    const rows = await dbCourse
        .collection("studentCourse")
        .findOne({
            siteId: +NEW_SITE_ID,
            oldIdUser: rawData?.IdUser,
            oldIdCourse: rawData?.IdCourse
        })
    return rows;
}


async function migrateTable(sqlConnection, mongoDbCourse, mongoDbClass, mongoDbUser, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    await deleteOldData(mongoDbCourse, "studentLesson")

    let offset = 0;
    // tạo lớp học 

    const userInRoom = {}
    const listLesson = await findListLesson(mongoDbCourse)
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const listUser = await findListUser(rows, mongoDbUser)

        const mappedStudentLesson = []

        for (const row of rows) {
            const userInMongo = listUser.find(item => item.mobieduUserId == row.IdUser)
            const userId = userInMongo?._id.toString()

            const lesson = listLesson.find(item => item.oldId == row.IdLesson)

            const studentCourse = await findStudentCourse(row, mongoDbCourse)

            if (!studentCourse) continue

            const newStudentLeson = createStudentLesson(row, lesson, userId, studentCourse)

            mappedStudentLesson.push(newStudentLeson);
        }
        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDbCourse.collection('studentLesson').insertMany(mappedStudentLesson);

        offset += +BATCH_SIZE;
    }

    const newClassRooms = classRooms.map((item) => ({
        ...item,
        students: userInRoom?.[item._id.toString()] || [],
        listIdsUser: userInRoom?.[item._id.toString()] || []
    }));
    await mongoDbClass.collection('classRoom').insertMany(newClassRooms);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_COURSE);
    const mongoDbClass = mongoClient.db('db_moocs_classes');
    const mongoDbUser = mongoClient.db(MONGO_DATABASE_USER)

    try {
        await migrateTable(sqlConnection, mongoDb, mongoDbClass, mongoDbUser, 'CompleteLessonLog');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();