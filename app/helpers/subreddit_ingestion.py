from app.utils.constants import CONFIG_PATH
import praw
from dataclasses import dataclass
import configparser


@dataclass
class Post:
    post_id: str
    url: str
    total_comments: int = 0
    c: int = 0
    post_score: float = 0.0


class SubredditIngestion:
    """
    A helper class for the secondary task IngestSubreddit, that performs all task specific operations

    """
    def __init__(self, subreddit_name, start, top_n_subreddits, top_n_posts, top_n_comments):
        self.subreddit_name = subreddit_name
        self.start = start
        self.top_n_subreddits = top_n_subreddits
        self.top_n_posts = top_n_posts
        self.top_n_comments = top_n_comments
        self.total_posts = 0
        self.subreddit_score = 0.0
        self.top_n_posts_list = []
        self.all_posts_list = []
        # TODO Change the constants below
        self.reddit = self.get_api_wrapper()

    def get_api_wrapper(self, config_path=CONFIG_PATH):
        config = configparser.ConfigParser()
        config.read(config_path)
        client_id = config['REDDIT']['client_id']
        client_secret = config['REDDIT']['client_secret']
        username = config['REDDIT']['username']
        password = config['REDDIT']['password']
        user_agent = config['REDDIT']['user_agent']

        return praw.Reddit(client_id=client_id,
                           client_secret=client_secret,
                           username=username,
                           password=password,
                           user_agent=user_agent)

    def derive_top_data(self):

        for post in self.reddit.subreddit(self.subreddit_name).top(limit=None):

            # Increment the total number of posts counter for final subreddit score calculation
            self.total_posts += 1

            # instantiate post object from post id and post url
            post_object = Post(post.id, post.url)
            all_comments_list = []

            submission = self.reddit.submission(id=post_object.post_id)
            submission.comments.replace_more(limit=None)
            for top_level_comment in submission.comments.list():

                # Increment the number of comments counter for each post
                post_object.total_comments += 1

                # Increment the total number of points for each post by adding comment upvotes to total upvotes
                post_object.post_score += top_level_comment.score

                comment_upvotes = top_level_comment.score if top_level_comment.score else 0
                all_comments_list.append(({"comment_body": top_level_comment.body, "comment_upvotes": comment_upvotes}))

            # sorting the comment list based on decreasing comment scores (Obtain only top 5 comments for ingestion)
            # Handle insufficient number of comments case:
            if len(all_comments_list) == 0:
                top_n_comments_list = []
            elif len(all_comments_list) > self.top_n_comments:
                top_n_comments_list = \
                    sorted(all_comments_list, key=lambda i: i['comment_upvotes'], reverse=True)[0:self.top_n_comments]
            else:
                top_n_comments_list = \
                    sorted(all_comments_list, key=lambda i: i['comment_upvotes'], reverse=True)

            # Calculate post score for each post from comment points
            try:
                post_object.post_score = post_object.post_score/post_object.total_comments
            # Handle cases when there are no comments for the post
            except ZeroDivisionError:
                post_object.post_score = 0

            # Populate list containing all posts for a subreddit
            self.all_posts_list.append({"post_id": post_object.post_id,
                                        "post_url": post_object.url,
                                        "post_score": post_object.post_score,
                                        "top_n_comments": top_n_comments_list})

            # Add post score to derive subreddit score
            self.subreddit_score += post_object.post_score

        # Calculate overall subreddit score:
        try:
            self.subreddit_score = self.subreddit_score/self.total_posts
        except ZeroDivisionError:
            self.subreddit_score = 0

        print("--------------self.top_n_posts---------------", self.top_n_posts)

        # Obtain only top n posts and save the data
        # Handle insufficient number of posts case:
        if len(self.all_posts_list) == 0:
            self.top_n_posts_list = []
        elif len(self.all_posts_list) > self.top_n_posts:
            self.top_n_posts_list = sorted(self.all_posts_list, key=lambda i: i['post_score'], reverse=True)[0:self.top_n_posts]
        else:
            self.top_n_posts_list = sorted(self.all_posts_list, key=lambda i: i['post_score'], reverse=True)

        subreddit_contents = {
            "subreddit": self.subreddit_name,
            "subreddit_score": self.subreddit_score,
            "top_contents": self.top_n_posts_list
        }

        return subreddit_contents


