export enum SseMessageType {
    text = "1002",       // 文本
    echart = "2001",     // 图表
    form = "2002",       // 表单
    digit = "2003",      // digit
    iframe = "2006",     // iframe
    task = "2008",       // task
}

export enum SseReasonMessageType {
    think_title = "3003",         // 思考过程标题
    think_sub_title = "3005",     // 思考过程子标题
    think_resource = "3004",      // 思考过程引用
    think_text = "1002",          // 思考过程文本
    think_code_answer = "3008",   // 思考过程的代码最终结果
    think_code = "3006",          // 思考过程的代码
    think_code_result = "3007",   // 思考过程的代码执行结果
    think_status_title = "3009",  // 思考过程标题(含状态)
    task_user_input = "3013",     // 用户输入
    task_create_file = "3010",    // 创建文件
    task_title = "3011",          // 任务标题
    agent_card = "2015",          // 智能体卡片
    async_card = "2014",          // 异步卡片
    json_block = "2020",          // Json 数据，包含title
}
