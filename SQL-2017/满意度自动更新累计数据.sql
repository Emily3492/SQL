StringBuffer sqlBuf = new StringBuffer();
        sqlBuf.append("select A1.APARTMENT_SID,A1.APARTMENT_NAME,A1.grade, A1.total * 10000 /a2.total tatal,A1.groupCase,a1.total as selftotal,a2.total as alltotal from ( ");
        //满意度
        sqlBuf.append("select  APARTMENT_SID ,APARTMENT_NAME, 'evaluation_item3' as groupCase,grade,COUNT(1) total FROM ( ");
        sqlBuf.append("SELECT hs.apartment_sid, ha.apartment_name,");
        sqlBuf.append("case when evaluation_item3 < 3 then 1 ");
        sqlBuf.append("when evaluation_item3 = 3 then 2 ");
        sqlBuf.append("when evaluation_item3 > 3 then 3 ");
        sqlBuf.append("else 0 end as grade  ");
        sqlBuf.append("from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid ");
        sqlBuf.append("where (service_status = 6 or service_status = 9) " +
                " and SERVICE_CATEGORY in('7D2B996C-12EC-4CD4-8499-B453E96AF11F'," +
                "'9098ED29-072D-4653-A37D-3C2F6DF80861'," +
                "'BCCF6994-9449-4E6D-9F5B-09CE08AD9353'," +
                "'C733AA3D-32FA-4F5B-B299-061044661DC0','51979B62-10E6-44C7-88B8-4B239B1CE03F')  " +
                " and evaluation_item3 is not null " +
                " and  hs.apartment_sid in (:apartmentSid)) b ");
        sqlBuf.append("group by apartment_sid,APARTMENT_NAME, grade ");
        //解决速度
        sqlBuf.append("union all ");
        sqlBuf.append("select  APARTMENT_SID ,APARTMENT_NAME, 'evaluation_item2' as groupCase,grade,COUNT(1) total FROM (  ");
        sqlBuf.append("SELECT hs.apartment_sid, ha.apartment_name,");
        sqlBuf.append("case when evaluation_item2 < 3 then 1 ");
        sqlBuf.append("when evaluation_item2 = 3 then 2 ");
        sqlBuf.append("when evaluation_item2 > 3 then 3 ");
        sqlBuf.append("else 0 end as grade  ");
        sqlBuf.append("from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid ");
        sqlBuf.append("where (service_status = 6 or service_status = 9) " +
                " and SERVICE_CATEGORY in('7D2B996C-12EC-4CD4-8499-B453E96AF11F'," +
                "'9098ED29-072D-4653-A37D-3C2F6DF80861'," +
                "'BCCF6994-9449-4E6D-9F5B-09CE08AD9353'," +
                "'C733AA3D-32FA-4F5B-B299-061044661DC0','51979B62-10E6-44C7-88B8-4B239B1CE03F')  " +
                " and evaluation_item2 is not null " +
                " and  hs.apartment_sid in (:apartmentSid)) b ");
        sqlBuf.append("group by apartment_sid,APARTMENT_NAME, grade ");
        //服务态度
        sqlBuf.append("union all ");
        sqlBuf.append("select  APARTMENT_SID ,APARTMENT_NAME, 'evaluation_item1' as groupCase,grade,COUNT(1) total FROM (  ");
        sqlBuf.append("SELECT hs.apartment_sid, ha.apartment_name,");
        sqlBuf.append("case when evaluation_item1 < 3 then 1 ");
        sqlBuf.append("when evaluation_item1 = 3 then 2 ");
        sqlBuf.append("when evaluation_item1 > 3 then 3 ");
        sqlBuf.append("else 0 end as grade  ");
        sqlBuf.append("from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid ");
        sqlBuf.append("where (service_status = 6 or service_status = 9) " +
                " and SERVICE_CATEGORY in('7D2B996C-12EC-4CD4-8499-B453E96AF11F'," +
                "'9098ED29-072D-4653-A37D-3C2F6DF80861'," +
                "'BCCF6994-9449-4E6D-9F5B-09CE08AD9353'," +
                "'C733AA3D-32FA-4F5B-B299-061044661DC0','51979B62-10E6-44C7-88B8-4B239B1CE03F')  " +
                " and evaluation_item1 is not null " +
                " and  hs.apartment_sid in (:apartmentSid)) b ");
        sqlBuf.append("group by apartment_sid,APARTMENT_NAME, grade  ) A1 ");

        sqlBuf.append("left join (");
        //满意度
        sqlBuf.append("select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total FROM ( ");
        sqlBuf.append("SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item3' as groupCase, ");
        sqlBuf.append("case when evaluation_item3 < 3 then 1 ");
        sqlBuf.append("when evaluation_item3 = 3 then 2 ");
        sqlBuf.append("when evaluation_item3 > 3 then 3 ");
        sqlBuf.append("else 0 end as grade  ");
        sqlBuf.append("from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid ");
        sqlBuf.append("where (service_status = 6 or service_status = 9) " +
                " and SERVICE_CATEGORY in('7D2B996C-12EC-4CD4-8499-B453E96AF11F'," +
                "'9098ED29-072D-4653-A37D-3C2F6DF80861'," +
                "'BCCF6994-9449-4E6D-9F5B-09CE08AD9353'," +
                "'C733AA3D-32FA-4F5B-B299-061044661DC0','51979B62-10E6-44C7-88B8-4B239B1CE03F')  " +
                " and evaluation_item3 is not null " +
                " and  hs.apartment_sid in (:apartmentSid)) c ");
        sqlBuf.append("group by apartment_sid,apartment_name,groupCase ");
        //解决速度
        sqlBuf.append("union all ");
        sqlBuf.append("select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (  ");
        sqlBuf.append("SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item2' as groupCase, ");
        sqlBuf.append("case when evaluation_item2 < 3 then 1 ");
        sqlBuf.append("when evaluation_item2 = 3 then 2 ");
        sqlBuf.append("when evaluation_item2 > 3 then 3 ");
        sqlBuf.append("else 0 end as grade  ");
        sqlBuf.append("from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid ");
        sqlBuf.append("where (service_status = 6 or service_status = 9) " +
                "and SERVICE_CATEGORY in('7D2B996C-12EC-4CD4-8499-B453E96AF11F'," +
                "'9098ED29-072D-4653-A37D-3C2F6DF80861'," +
                "'BCCF6994-9449-4E6D-9F5B-09CE08AD9353'," +
                "'C733AA3D-32FA-4F5B-B299-061044661DC0','51979B62-10E6-44C7-88B8-4B239B1CE03F')  " +
                " and evaluation_item2 is not null " +
                " and  hs.apartment_sid in (:apartmentSid)) c ");
        sqlBuf.append("group by apartment_sid,apartment_name,groupCase ");
        //服务态度
        sqlBuf.append("union all ");
        sqlBuf.append("select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (  ");
        sqlBuf.append("SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item1' as groupCase, ");
        sqlBuf.append("case when evaluation_item1 < 3 then 1 ");
        sqlBuf.append("when evaluation_item1 = 3 then 2 ");
        sqlBuf.append("when evaluation_item1 > 3 then 3 ");
        sqlBuf.append("else 0 end as grade  ");
        sqlBuf.append("from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid ");
        sqlBuf.append("where (service_status = 6 or service_status = 9) " +
                " and SERVICE_CATEGORY in('7D2B996C-12EC-4CD4-8499-B453E96AF11F'," +
                "'9098ED29-072D-4653-A37D-3C2F6DF80861'," +
                "'BCCF6994-9449-4E6D-9F5B-09CE08AD9353'," +
                "'C733AA3D-32FA-4F5B-B299-061044661DC0','51979B62-10E6-44C7-88B8-4B239B1CE03F')  " +
                " and evaluation_item1 is not null " +
                " and  hs.apartment_sid in (:apartmentSid)) c ");
        sqlBuf.append("group by apartment_sid,apartment_name,groupCase  ) A2 ");
        sqlBuf.append("on A1.apartment_sid = A2.apartment_sid and a1.groupCase=a2.groupCase order by A1.apartment_sid");

        Query query = entityManager.createNativeQuery(sqlBuf.toString());
        query.setParameter("apartmentSid", asid);
        return query.getResultList();
