import faust


app = faust.App(
    'page_views',
    broker='kafka://broker:9092',
    topic_partitions=4,
)

# class PostActionCounter(faust.Record):
#     post_id: str
#     user_id: str
#     action_type: str

# post_action_topic = app.topic('post_actions', value_type=PostActionCounter)
# post_actions = app.Table('post_actions', default=int)

# @app.agent(post_action_topic)
# async def count_post_actions(views):
#     print("views----------------",views)
#     async for view in views.group_by(PostActionCounter.post_id):
#         print("@@@",view)
#         post_actions[view.post_id] += 1

class PageView(faust.Record):
    id: str
    user: str

page_view_topic = app.topic('page_views', value_type=PageView)
page_views = app.Table('page_views', default=int)

@app.agent(page_view_topic)
async def count_page_views(views):
    async for view in views.group_by(PageView.id):
        page_views[view.id] += 1

