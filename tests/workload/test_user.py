from defio.workload.user import User


def test_random_user() -> None:
    num_users = 100
    random_users = {User.random() for _ in range(num_users)}

    # Each user should be unique (by equality)
    assert len(random_users) == num_users


def test_with_label() -> None:
    label = "tim"
    user_1, user_2 = User.with_label(label), User.with_label(label)

    # `with_label` returns random user as well
    assert user_1 != user_2


def test_relabel() -> None:
    random_user = User.random()
    labeled_user = random_user.relabel(label := 0)

    assert random_user.label is None
    assert labeled_user.label == label
    assert random_user == labeled_user
