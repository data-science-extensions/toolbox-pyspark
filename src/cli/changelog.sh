origin_url=$(git remote get-url origin | sed "s|\.git|\/|g")
repo=$(echo $origin_url | sed -E 's|.*[/:]([^/]+/[^/]+)/|\1|')
previous_tag=0

printf "<!-- This file is auto-generated. Do not edit this file manually -->\n\n"

printf "# Changelog\n\n"

for current_tag in $(git tag --sort=-creatordate)
do

    if [ "$previous_tag" != 0 ]
    then
        tag_date=$(git log -1 --pretty=format:'%ad' --date=short ${previous_tag})
        release_title=$(curl -s "https://api.github.com/repos/${repo}/releases/tags/${previous_tag}" | jq -r '.name')
        printf "## ${release_title}\n\n"
        printf "🏷️ Release Tag: \`${previous_tag}\`<br>"
        printf "📅 Release Date: \`${tag_date}\`\n\n"
        git log ${current_tag}...${previous_tag} --pretty=format:'* %s (by [%an](mailto:%ae)) [View](GIT_URL/-/commit/%H)' --reverse | grep -v "Merge" | grep -v "Bump" | grep -v "Update coverage report" | grep -v "Update changelog" | sed "s|GIT_URL|${origin_url}|g"
        printf "\n\n"
    fi

previous_tag=${current_tag}
done
